"""Example script demonstrating writing and reading Parquet files using parquet_demo.io.

This script supports several modes:
- default: writes a small in-memory table to Parquet and reads it back
- --mongo-uri: fetch from MongoDB and write to Parquet
- --to-mongo --parquet-path PATH: read a Parquet file and insert its rows into MongoDB
"""

from pathlib import Path
import tempfile
import os
import argparse
from typing import Iterable

import pyarrow as pa

# When running this example directly (python examples/run_example.py) ensure the
# project's `src/` directory is on sys.path so the `parquet_demo` package can be
# imported without installing the project.
import sys
from pathlib import Path as _Path
_root = _Path(__file__).resolve().parents[1]
_src = str((_root / 'src').resolve())
if _src not in sys.path:
    sys.path.insert(0, _src)

from parquet_demo import io

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover - optional dependency
    MongoClient = None

# Optional: pymongoarrow provides efficient conversion between MongoDB and Arrow
try:
    # pymongoarrow package layout may vary; try to import common helpers
    from pymongoarrow.api import find_arrow_all, find_pandas_all  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    find_arrow_all = None
    find_pandas_all = None


def fetch_from_mongo(uri: str, db: str, collection: str, limit: int | None = 1000):
    """Fetch documents from MongoDB and return a list of dicts (simple, no nested handling)."""
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed; install requirements.txt to enable MongoDB support")

    client = MongoClient(uri)
    coll = client[db][collection]

    # Prefer pymongoarrow if available for an Arrow-native read
    if find_arrow_all is not None:
        try:
            # find_arrow_all typically accepts a collection-like object
            table = find_arrow_all(coll)
            return table.to_pylist()
        except Exception:
            # If pymongoarrow call fails for any reason, fall back to pymongo
            pass

    if find_pandas_all is not None:
        try:
            df = find_pandas_all(coll)
            # Convert pandas DataFrame to list of dicts
            return df.to_dict(orient='records')
        except Exception:
            pass

    # Fallback: use pymongo's find
    docs = list(coll.find({}, limit=limit))
    # Convert ObjectId to string and move to 'id' key
    for d in docs:
        if '_id' in d:
            d['id'] = str(d.pop('_id'))
    return docs


def infer_schema_from_docs(docs: list[dict]) -> pa.Schema:
    """Infer a very small Arrow schema from a list of flat dict documents.

    This is intentionally simple: it inspects the first document and maps Python types
    to a reasonable Arrow type. Nested structures are not supported here.
    """
    if not docs:
        return pa.schema([])

    first = docs[0]
    fields = []
    import datetime as _dt

    for k, v in first.items():
        # Integers and floats map to numeric Arrow types
        if isinstance(v, int):
            t = pa.int64()
        elif isinstance(v, float):
            t = pa.float64()
        # datetime.datetime -> timestamp with microsecond resolution
        elif isinstance(v, _dt.datetime):
            t = pa.timestamp('us')
        # datetime.date -> date32
        elif isinstance(v, _dt.date):
            t = pa.date32()
        else:
            t = pa.string()
        fields.append(pa.field(k, t))
    return pa.schema(fields)


def write_to_mongo(parquet_path: str, uri: str, db: str, collection: str, batch_size: int = 1000):
    """Read a Parquet file and insert rows into MongoDB in batches.

    parquet_path: path to parquet file
    uri: MongoDB connection URI
    db: database name
    collection: collection name
    batch_size: number of docs to insert per batch
    """
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed; install requirements.txt to enable MongoDB support")

    table = io.read_table(parquet_path)
    # pyarrow Table -> list[dict] with native Python types and None for nulls
    records = table.to_pylist()

    client = MongoClient(uri)
    coll = client[db][collection]

    # Insert in batches
    total = 0
    batch: list[dict] = []
    for rec in records:
        # If there's an 'id' field that is a string and looks like an ObjectId, leave it
        batch.append(rec)
        if len(batch) >= batch_size:
            coll.insert_many(batch)
            total += len(batch)
            batch = []

    if batch:
        coll.insert_many(batch)
        total += len(batch)

    print(f'Inserted {total} documents into {db}.{collection}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo-uri', help='MongoDB connection URI (or set MONGO_URI env var)')
    parser.add_argument('--mongo-db', default='test', help='MongoDB database name')
    parser.add_argument('--mongo-collection', default='sample', help='MongoDB collection name')
    parser.add_argument('--out', help='Path to write parquet file; default: temp file')
    parser.add_argument('--limit', type=int, default=1000, help='Max documents to fetch from MongoDB')
    parser.add_argument('--to-mongo', action='store_true', help='Read parquet and insert into MongoDB')
    parser.add_argument('--parquet-path', help='Path to parquet file to read when using --to-mongo')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for MongoDB inserts')
    args = parser.parse_args()

    mongo_uri = args.mongo_uri or os.environ.get('MONGO_URI')

    if args.to_mongo:
        if not mongo_uri:
            raise SystemExit('MongoDB URI required for --to-mongo (set MONGO_URI or pass --mongo-uri)')
        if not args.parquet_path:
            raise SystemExit('Please provide --parquet-path when using --to-mongo')
        write_to_mongo(args.parquet_path, mongo_uri, args.mongo_db, args.mongo_collection, batch_size=args.batch_size)
        return

    if mongo_uri:
        docs = fetch_from_mongo(mongo_uri, args.mongo_db, args.mongo_collection, limit=args.limit)
        if not docs:
            print('No documents found in collection; exiting')
            return

        schema = infer_schema_from_docs(docs)
        rows = docs
    else:
        # fallback in-memory demo
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
            pa.field('score', pa.float64()),
        ])

        rows = [
            {'id': 1, 'name': 'Alice', 'score': 9.5},
            {'id': 2, 'name': 'Bob', 'score': 7.3},
            {'id': 3, 'name': 'Carol', 'score': 8.1},
        ]

    out_path = args.out
    if out_path:
        out_path = Path(out_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / 'sample.parquet' if out_path is None else out_path
        io.write_table(str(path), schema, rows)
        table = io.read_table(str(path))
        print('Wrote and read table:')
        print(table.to_pandas())


if __name__ == '__main__':
    main()
