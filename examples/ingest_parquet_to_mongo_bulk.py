"""Bulk ingest a Parquet file into MongoDB using bulk_write for higher throughput.

Behaviors:
- Converts decimal.Decimal to bson.Decimal128 when available.
- Converts numpy types and numpy.datetime64 to Python-native types.
- Uses pymongo bulk_write with InsertOne or ReplaceOne (when --upsert) operations.
- Supports --dry-run to preview conversions without writing.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import decimal
from pathlib import Path
import time
from typing import Any, Dict, List, NamedTuple
from queue import Queue

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from pymongo import MongoClient, WriteConcern
    from pymongo import InsertOne, ReplaceOne
    from bson.decimal128 import Decimal128
except Exception:  # pragma: no cover - optional dependency
    MongoClient = None
    WriteConcern = None
    InsertOne = None
    ReplaceOne = None
    Decimal128 = None


class Metrics(NamedTuple):
    """Collect performance metrics for a batch operation."""
    batch_size: int
    conversion_ms: float
    write_ms: float
    rows_per_sec: float


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


def _convert_batch(records: List[Dict[str, Any]], schema: pa.Schema) -> List[Dict[str, Any]]:
    """Convert a batch of records using column-wise operations for better performance."""
    if not records:
        return []

    # Extract columns as lists for faster conversion
    names = list(records[0].keys())
    cols = [[r[name] for r in records] for name in names]
    field_types = {f.name: f.type for f in schema}

    # Convert columns efficiently
    converted_cols = []
    for name, col in zip(names, cols):
        if pa.types.is_decimal(field_types[name]):
            converted = [Decimal128(str(v)) if v is not None and Decimal128 is not None
                       else str(v) if v is not None else None for v in col]
        elif pa.types.is_timestamp(field_types[name]):
            converted = [_np_datetime64_to_py(v) if isinstance(v, np.datetime64)
                       else (v.astimezone(timezone.utc).replace(tzinfo=None)
                            if v is not None and v.tzinfo is not None else v)
                       for v in col]
        else:
            converted = [v.item() if isinstance(v, np.generic) else v for v in col]
        converted_cols.append(converted)

    # Build documents using zip
    return [dict(zip(names, values)) for values in zip(*converted_cols)]


def _convert_and_submit(records: List[Dict[str, Any]], schema: pa.Schema,
                       queue: Queue, batch_size: int, upsert: bool,
                       pk_fields: List[str] | None = None) -> None:
    """Convert a batch of records and submit to the write queue."""
    t0 = time.perf_counter()
    converted = _convert_batch(records, schema)
    t1 = time.perf_counter()

    # Build ops if needed
    if upsert and pk_fields:
        ops = []
        for doc in converted:
            filt = {k: doc[k] for k in pk_fields}
            ops.append(ReplaceOne(filt, doc, upsert=True))
        queue.put((ops, t1 - t0, len(converted)))
    else:
        # For pure inserts, just queue the converted docs
        queue.put((converted, t1 - t0, len(converted)))


def bulk_ingest(parquet_path: Path, mongo_uri: str | None, db: str, collection: str,
                batch_size: int = 10000, dry_run: bool = False, upsert: bool = False,
                pk_fields: List[str] | None = None, max_workers: int = 4,
                max_queue_size: int = 20):
    """Improved bulk ingest with parallel conversion and metrics."""
    table = pq.read_table(parquet_path.as_posix())
    rows = table.to_pylist()
    schema = table.schema

    if dry_run:
        converted = _convert_batch(rows[:5], schema)
        for i, (orig, conv) in enumerate(zip(rows[:5], converted)):
            print('Original[{}]:'.format(i), orig)
            print('Converted[{}]:'.format(i), conv)
        print('Total rows:', len(rows))
        return

    if MongoClient is None:
        raise RuntimeError('pymongo is not installed; install requirements to enable MongoDB ingestion')

    # Use write concern w=1 for better throughput
    client = MongoClient(mongo_uri)
    wc = WriteConcern(w=1)
    coll = client[db][collection].with_options(write_concern=wc)

    # Queue for passing converted batches to writer
    queue: Queue = Queue(maxsize=max_queue_size)
    total = 0
    metrics: List[Metrics] = []

    # Start conversion pool
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # Submit initial conversion jobs
        futures = []
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            fut = pool.submit(_convert_and_submit, batch, schema, queue,
                            batch_size, upsert, pk_fields)
            futures.append(fut)

        # Process converted batches as they arrive
        completed = 0
        while completed < len(futures):
            try:
                batch_data, conv_time, batch_count = queue.get(timeout=1.0)
                t0 = time.perf_counter()

                if upsert:
                    # batch_data contains ReplaceOne ops
                    res = coll.bulk_write(batch_data, ordered=False)
                    count = sum(getattr(res, name, 0) for name in
                              ('inserted_count', 'modified_count', 'upserted_count'))
                else:
                    # batch_data contains raw docs
                    res = coll.insert_many(batch_data, ordered=False)
                    count = len(res.inserted_ids)

                t1 = time.perf_counter()
                write_time = t1 - t0
                
                # Collect metrics
                total += count
                if count > 0:  # avoid div by zero
                    rows_per_sec = count / write_time
                    metrics.append(Metrics(count, conv_time * 1000,
                                        write_time * 1000, rows_per_sec))
                
                print(f'Wrote {count} docs (total {total}, '
                      f'conv: {conv_time*1000:.1f}ms, '
                      f'write: {write_time*1000:.1f}ms, '
                      f'rate: {rows_per_sec:.0f} rows/sec)')

            except TimeoutError:
                # Check for completed futures
                done = [f for f in futures if f.done()]
                completed = len(done)
                continue

    # Print summary metrics
    if metrics:
        conv_times = [m.conversion_ms for m in metrics]
        write_times = [m.write_ms for m in metrics]
        rates = [m.rows_per_sec for m in metrics]
        print('\nSummary:')
        print(f'Total rows: {total}')
        print(f'Conversion time (ms): med={median(conv_times):.1f}, '
              f'avg={mean(conv_times):.1f}')
        print(f'Write time (ms): med={median(write_times):.1f}, '
              f'avg={mean(write_times):.1f}')
        print(f'Throughput: {mean(rates):.0f} rows/sec '
              f'(peak: {max(rates):.0f})')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet', type=Path, required=True)
    parser.add_argument('--mongo-uri', type=str, default=None)
    parser.add_argument('--db', type=str, default='test')
    parser.add_argument('--collection', type=str, default='supplymethod')
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Number of parallel conversion workers')
    parser.add_argument('--max-queue-size', type=int, default=20,
                       help='Maximum number of batches to queue')
    parser.add_argument('--upsert', action='store_true',
                       help='Use ReplaceOne upsert instead of InsertOne')
    parser.add_argument('--pk-fields', type=str, default=None,
                       help='Comma-separated PK field names to use for upsert')
    args = parser.parse_args(argv)

    time_start = time.time()

    pk_fields = args.pk_fields.split(',') if args.pk_fields else None
    bulk_ingest(args.parquet, args.mongo_uri, args.db, args.collection,
                batch_size=args.batch_size, dry_run=args.dry_run,
                upsert=args.upsert, pk_fields=pk_fields,
                max_workers=args.max_workers,
                max_queue_size=args.max_queue_size)
    
    time_end = time.time()
    print(f'Total elapsed time: {time_end - time_start:.2f}s')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
