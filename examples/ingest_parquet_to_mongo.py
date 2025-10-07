"""Ingest a Parquet file into MongoDB.

Converts pyarrow/pyarrow-derived Python types to MongoDB-friendly types:
- decimal.Decimal -> bson.decimal128.Decimal128
- numpy types -> native Python types
- numpy.datetime64 -> python datetime (UTC, naive)
- timezone-aware datetimes -> converted to naive UTC datetimes

Supports a --dry-run mode to preview converted documents without connecting to MongoDB.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import decimal
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from pymongo import MongoClient
    from bson.decimal128 import Decimal128
except Exception:  # pragma: no cover - optional dependency
    MongoClient = None
    Decimal128 = None


def _np_datetime64_to_py(v: np.datetime64) -> datetime:
    # convert to microsecond-resolution integer since epoch
    us = int(v.astype('datetime64[us]').astype('int64'))
    return datetime.utcfromtimestamp(us / 1_000_000)


def _convert_value(v: Any) -> Any:
    # None
    if v is None:
        return None

    # decimal.Decimal -> bson Decimal128
    if isinstance(v, decimal.Decimal):
        if Decimal128 is None:
            # return string fallback
            return str(v)
        return Decimal128(str(v))

    # numpy scalar -> Python native
    if isinstance(v, np.generic):
        try:
            return v.item()
        except Exception:
            # fallback to Python conversion
            return v.tolist()

    # numpy.datetime64
    if isinstance(v, np.datetime64):
        return _np_datetime64_to_py(v)

    # datetime -> ensure naive UTC (Mongo stores naive datetimes but in UTC)
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v

    # default: return as-is
    return v


def convert_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        out[k] = _convert_value(v)
    return out


def ingest(parquet_path: Path, mongo_uri: str | None, db: str, collection: str, batch_size: int = 1000, dry_run: bool = False):
    table = pq.read_table(parquet_path.as_posix())
    rows = table.to_pylist()

    if dry_run:
        # show a few converted documents and return
        for i, r in enumerate(rows[:min(5, len(rows))]):
            print(f"Original[{i}]:", r)
            print(f"Converted[{i}]:", convert_record(r))
        print(f"Total rows available in file: {len(rows)}")
        return

    if MongoClient is None:
        raise RuntimeError("pymongo is not installed; install requirements to enable MongoDB ingestion")

    client = MongoClient(mongo_uri)
    coll = client[db][collection]

    batch = []
    total = 0
    for r in rows:
        batch.append(convert_record(r))
        if len(batch) >= batch_size:
            coll.insert_many(batch)
            total += len(batch)
            print(f"Inserted {total} documents...")
            batch = []

    if batch:
        coll.insert_many(batch)
        total += len(batch)
        print(f"Inserted {total} documents (final)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet', type=Path, required=True)
    parser.add_argument('--mongo-uri', type=str, default=None, help='MongoDB URI or set MONGO_URI env var')
    parser.add_argument('--db', type=str, default='test')
    parser.add_argument('--collection', type=str, default='supplymethod')
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--dry-run', action='store_true', help='Do not insert; print sample converted documents')
    args = parser.parse_args(argv)

    ingest(args.parquet, args.mongo_uri, args.db, args.collection, batch_size=args.batch_size, dry_run=args.dry_run)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
