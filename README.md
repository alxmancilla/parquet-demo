# parquet-demo

Minimal demo for reading and writing Parquet files using PyArrow, plus small examples that show how to move data between MongoDB and Parquet.

This repository contains:

- `src/parquet_demo/io.py` — small helpers to write/read Parquet with PyArrow
- `examples/run_example.py` — CLI with modes:
  - default: write a tiny in-memory table to Parquet and read it back
  - `--mongo-uri`: fetch from MongoDB and write to Parquet
  - `--to-mongo --parquet-path PATH`: read a Parquet file and insert rows into MongoDB
- `tests/` — pytest tests for basic functionality

Requirements

- Python 3.8+
- pyarrow
- pymongo (only required for MongoDB-related examples)
 - pymongoarrow (optional) - if installed, examples will attempt to use it for faster Arrow-native reads from MongoDB

Quick start (macOS, zsh)

Create and activate a virtual environment, then install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the default example (in-memory demo):

```bash
python examples/run_example.py
```

MongoDB -> Parquet

Fetch documents from a MongoDB collection and write to a Parquet file. Provide a MongoDB URI via the `--mongo-uri` flag or `MONGO_URI` env var.

```bash
export MONGO_URI='mongodb://user:pass@localhost:27017'
python examples/run_example.py --mongo-uri "$MONGO_URI" --mongo-db mydb --mongo-collection mycollection --out ./out.parquet
```

Parquet -> MongoDB

Read a Parquet file and insert its rows into a MongoDB collection (batch insert):

```bash
export MONGO_URI='mongodb://user:pass@localhost:27017'
python examples/run_example.py --to-mongo --parquet-path ./out.parquet --mongo-uri "$MONGO_URI" --mongo-db mydb --mongo-collection mycollection
```

Notes and caveats

- The schema inference used for MongoDB->Parquet is intentionally simple: it maps ints -> int64, floats -> float64, and all other values to string. Nested documents and arrays are not supported in the helper.
- The Parquet->Mongo insertion performs naive insert_many calls. If you need idempotency, consider implementing upserts or `_id` mapping.
- If you get editor lint warnings like "Import could not be resolved" for `pyarrow` or `pymongo`, install the requirements in your venv and the warnings should go away.

Running tests

```bash
pytest -q
```

Suggested next steps

- Move MongoDB helpers into `src/parquet_demo/mongo.py` and add unit tests that use `mongomock` to avoid needing a real MongoDB instance in CI.
- Add a CLI flag to control whether an `id` column maps to `_id` in MongoDB or is kept as a regular field.
- Add a small GitHub Actions workflow to run tests on push.

License: MIT
