"""Stream a Parquet file into MongoDB using ParquetFile.iter_batches().

This minimizes memory usage by processing Arrow RecordBatches in a streaming
fashion and converting each batch to Python dicts for insertion.

Supports --dry-run, --batch-size, --upsert, and --pk-fields.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import decimal
from pathlib import Path
import time
from typing import Any, Dict, List, NamedTuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from queue import Queue
from statistics import mean, median

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from pymongo import MongoClient
    from pymongo import InsertOne, ReplaceOne
    from bson.decimal128 import Decimal128
except Exception:  # pragma: no cover - optional dependency
    MongoClient = None
    InsertOne = None
    ReplaceOne = None
    Decimal128 = None


def _np_datetime64_to_py(v: np.datetime64) -> datetime:
    us = int(v.astype('datetime64[us]').astype('int64'))
    return datetime.utcfromtimestamp(us / 1_000_000)


def _convert_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, decimal.Decimal):
        if Decimal128 is None:
            return str(v)
        return Decimal128(str(v))
    if isinstance(v, np.generic):
        try:
            return v.item()
        except Exception:
            return v.tolist()
    if isinstance(v, np.datetime64):
        return _np_datetime64_to_py(v)
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v
    return v


def _make_column_converter(field: pa.Field) -> callable:
    """Build an optimized converter function for a column based on its Arrow type."""
    if pa.types.is_decimal(field.type):
        def convert_decimal(arr):
            # Convert decimal column to Decimal128 or str in one pass
            return [Decimal128(str(v)) if v is not None and Decimal128 is not None 
                   else str(v) if v is not None else None for v in arr]
        return convert_decimal
    elif pa.types.is_timestamp(field.type):
        def convert_datetime(arr):
            # Convert timestamp column to datetime in one pass
            return [_np_datetime64_to_py(v) if isinstance(v, np.datetime64)
                   else (v.astimezone(timezone.utc).replace(tzinfo=None)
                        if v is not None and v.tzinfo is not None else v)
                   for v in arr]
        return convert_datetime
    else:
        def convert_generic(arr):
            # Handle numpy scalars and arrays in one pass
            return [v.item() if isinstance(v, np.generic) else v for v in arr]
        return convert_generic


def _recordbatch_to_docs(rb: pa.RecordBatch) -> List[Dict[str, Any]]:
    """Convert a RecordBatch to a list of MongoDB-compatible documents using
    column-wise operations and zip for better performance."""
    table = pa.Table.from_batches([rb])
    records = table.to_pydict()

    # Get field names and build column converters
    names = list(records.keys())
    cols = [records[n] for n in names]
    converters = [_make_column_converter(field) for field in table.schema]

    # Apply converters column-wise (faster than row-wise conversion)
    converted = [conv(col) for conv, col in zip(converters, cols)]

    # Build documents using zip (faster than nested loops)
    return [dict(zip(names, row)) for row in zip(*converted)]


def stream_ingest(parquet_path: Path, mongo_uri: str | None, db: str, collection: str, batch_size: int = 1000, dry_run: bool = False, upsert: bool = False, pk_fields: List[str] | None = None, row_batch_size: int = 10000):
    pf = pq.ParquetFile(parquet_path.as_posix())
    if dry_run:
        # read only first batch and print samples
        for rb in pf.iter_batches(batch_size=row_batch_size):
            docs = _recordbatch_to_docs(rb)
            for i, d in enumerate(docs[:5]):
                print('Original[{}]:'.format(i), d)
            print('Total rows in sample batch:', len(docs))
            return

    if MongoClient is None:
        raise RuntimeError('pymongo is not installed; install requirements to enable MongoDB ingestion')

    client = MongoClient(mongo_uri)
    coll = client[db][collection]

    # We'll collect operation batches and submit them to a thread pool so multiple
    # write calls can run concurrently. For insert-only workloads we use
    # `insert_many(docs, ordered=False)` which is typically faster than
    # building individual InsertOne ops + bulk_write; for upserts we keep
    # ReplaceOne + bulk_write.
    op_batches: List[Any] = []
    docs_batches: List[Any] = []
    total = 0
    futures = []

    max_workers = getattr(stream_ingest, '__max_workers__', 4)
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        def submit_ops(batch_ops: List[Any]):
            # Submit a bulk_write for op objects (ReplaceOne etc.)
            return exe.submit(lambda b: coll.bulk_write(b, ordered=False), batch_ops)

        def submit_docs(batch_docs: List[Dict[str, Any]]):
            # Submit an insert_many for pure insert batches
            return exe.submit(lambda d: coll.insert_many(d, ordered=False), batch_docs)

        for rb in pf.iter_batches(batch_size=row_batch_size):
            docs = _recordbatch_to_docs(rb)
            for doc in docs:
                if upsert and pk_fields:
                    filt = {k: doc[k] for k in pk_fields}
                    op_batches.append(ReplaceOne(filt, doc, upsert=True))
                else:
                    docs_batches.append(doc)

                if len(op_batches) >= batch_size:
                    futures.append(submit_ops(op_batches))
                    op_batches = []

                if len(docs_batches) >= batch_size:
                    futures.append(submit_docs(docs_batches))
                    docs_batches = []

        # submit remaining
        if op_batches:
            futures.append(submit_ops(op_batches))
        if docs_batches:
            futures.append(submit_docs(docs_batches))

        # Wait for futures and aggregate results
        for fut in as_completed(futures):
            try:
                res = fut.result()
                # insert_many returns InsertManyResult with inserted_ids
                if hasattr(res, 'inserted_ids'):
                    count = len(getattr(res, 'inserted_ids', []) or [])
                    total += count
                    print(f'Inserted {count} docs (total {total})')
                    continue

                # bulk_write result: try to extract useful counters
                count = sum(getattr(res, name, 0) for name in ('inserted_count', 'modified_count', 'upserted_count')) or None
                if count is None:
                    print('Bulk batch completed (unknown count)')
                else:
                    total += count
                    print(f'Bulk wrote {count} docs (total {total})')
            except Exception as e:
                print('Write failed:', e)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet', type=Path, required=True)
    parser.add_argument('--mongo-uri', type=str, default=None)
    parser.add_argument('--db', type=str, default='test')
    parser.add_argument('--collection', type=str, default='supplymethod')
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--row-batch-size', type=int, default=10000, help='Arrow row-batch size to iterate')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of concurrent bulk write workers')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--upsert', action='store_true')
    parser.add_argument('--pk-fields', type=str, default=None, help='Comma-separated PK field names to use for upsert')
    args = parser.parse_args(argv)

    time_start = time()

    pk_fields = args.pk_fields.split(',') if args.pk_fields else None
    # attach max workers to the function object so the implementation can read it
    setattr(stream_ingest, '__max_workers__', args.max_workers)
    stream_ingest(args.parquet, args.mongo_uri, args.db, args.collection, batch_size=args.batch_size, dry_run=args.dry_run, upsert=args.upsert, pk_fields=pk_fields, row_batch_size=args.row_batch_size)
    
    time_end = time()
    print(f'Total elapsed time: {time_end - time_start:.2f}s')
    
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
