"""Bulk ingest a Parquet file into MongoDB using bulk_write for higher throughput.

Behaviors:
- Converts decimal.Decimal to bson.Decimal128 when available.
- Converts numpy types and numpy.datetime64 to Python-native types.
- Uses pymongo bulk_write with InsertOne or ReplaceOne (when --upsert) operations.
- Supports --dry-run to preview conversions without writing.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import decimal
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
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


def _convert_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _convert_value(v) for k, v in rec.items()}


def bulk_ingest(parquet_path: Path, mongo_uri: str | None, db: str, collection: str, batch_size: int = 10000, dry_run: bool = False, upsert: bool = False, pk_fields: List[str] | None = None):
    table = pq.read_table(parquet_path.as_posix())
    rows = table.to_pylist()

    if dry_run:
        for i, r in enumerate(rows[:min(5, len(rows))]):
            print('Original[{}]:'.format(i), r)
            print('Converted[{}]:'.format(i), _convert_record(r))
        print('Total rows:', len(rows))
        return

    if MongoClient is None:
        raise RuntimeError('pymongo is not installed; install requirements to enable MongoDB ingestion')

    client = MongoClient(mongo_uri)
    coll = client[db][collection]

    ops = []
    total = 0
    for r in rows:
        doc = _convert_record(r)
        if upsert and pk_fields:
            # Build a filter dict from pk_fields
            filt = {k: doc[k] for k in pk_fields}
            ops.append(ReplaceOne(filt, doc, upsert=True))
        else:
            ops.append(InsertOne(doc))

        if len(ops) >= batch_size:
            res = coll.bulk_write(ops, ordered=False)
            total += res.inserted_count if hasattr(res, 'inserted_count') else len(ops)
            print(f'Bulk wrote {total} documents (last batch: {len(ops)})')
            ops = []

    if ops:
        res = coll.bulk_write(ops, ordered=False)
        total += res.inserted_count if hasattr(res, 'inserted_count') else len(ops)
        print(f'Final bulk wrote, total {total}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet', type=Path, required=True)
    parser.add_argument('--mongo-uri', type=str, default=None)
    parser.add_argument('--db', type=str, default='test')
    parser.add_argument('--collection', type=str, default='supplymethod')
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--upsert', action='store_true', help='Use ReplaceOne upsert instead of InsertOne')
    parser.add_argument('--pk-fields', type=str, default=None, help='Comma-separated PK field names to use for upsert')
    args = parser.parse_args(argv)

    pk_fields = args.pk_fields.split(',') if args.pk_fields else None
    bulk_ingest(args.parquet, args.mongo_uri, args.db, args.collection, batch_size=args.batch_size, dry_run=args.dry_run, upsert=args.upsert, pk_fields=pk_fields)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
