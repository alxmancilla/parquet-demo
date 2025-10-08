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
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def _recordbatch_to_docs(rb: pa.RecordBatch) -> List[Dict[str, Any]]:
    # Convert RecordBatch to python list[dict] while converting values
    table = pa.Table.from_batches([rb])
    records = table.to_pydict()
    # records: dict[field] -> list(values)
    n = len(next(iter(records.values()))) if records else 0
    docs: List[Dict[str, Any]] = []
    for i in range(n):
        doc: Dict[str, Any] = {}
        for k, arr in records.items():
            val = arr[i]
            doc[k] = _convert_value(val)
        docs.append(doc)
    return docs


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
    # bulk_write calls can run concurrently. This helps saturate the MongoDB
    # server when network or server parallelism is the bottleneck.
    ops: List[Any] = []
    total = 0
    futures = []

    max_workers = getattr(stream_ingest, '__max_workers__', 4)
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        def submit_ops(batch_ops: List[Any]):
            # Submit the bulk_write call to the executor and return its future
            return exe.submit(lambda b: coll.bulk_write(b, ordered=False), batch_ops)

        for rb in pf.iter_batches(batch_size=row_batch_size):
            docs = _recordbatch_to_docs(rb)
            for doc in docs:
                if upsert and pk_fields:
                    filt = {k: doc[k] for k in pk_fields}
                    ops.append(ReplaceOne(filt, doc, upsert=True))
                else:
                    ops.append(InsertOne(doc))

                if len(ops) >= batch_size:
                    futures.append(submit_ops(ops))
                    ops = []

        if ops:
            futures.append(submit_ops(ops))

        # Wait for futures and aggregate results
        for fut in as_completed(futures):
            try:
                res = fut.result()
                count = sum(getattr(res, name, 0) for name in ('inserted_count', 'modified_count', 'upserted_count')) or None
                # best-effort: if we don't get counts from result, approximate by batch_size
                if count is None:
                    # we cannot know the batch size here; skip increment
                    print('Bulk batch completed (unknown count)')
                else:
                    total += count
                    print(f'Bulk wrote {total} documents (this batch {count})')
            except Exception as e:
                print('Bulk write failed:', e)


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

    pk_fields = args.pk_fields.split(',') if args.pk_fields else None
    # attach max workers to the function object so the implementation can read it
    setattr(stream_ingest, '__max_workers__', args.max_workers)
    stream_ingest(args.parquet, args.mongo_uri, args.db, args.collection, batch_size=args.batch_size, dry_run=args.dry_run, upsert=args.upsert, pk_fields=pk_fields, row_batch_size=args.row_batch_size)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
