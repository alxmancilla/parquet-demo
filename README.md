# parquet-demo

Minimal demo for reading and writing Parquet files using PyArrow, with examples showing how to generate large Parquet files and move data between MongoDB and Parquet.

Repository highlights

- `src/parquet_demo/io.py` — small helpers to write/read Parquet with PyArrow
- `examples/run_example.py` — small interactive demo (in-memory) and Mongo<->Parquet helpers
- `examples/generate_big_parquet.py` — generate a Parquet file with 50 fields and a configurable number of rows (default: 1_000_000)
- `examples/generate_parquet_from_ddl.py` — generate a Parquet file matching the provided DDL schema (streamed row-groups)
- `examples/ingest_parquet_to_mongo.py` — read Parquet and insert documents into MongoDB (batched, with --dry-run)
- `examples/ingest_parquet_to_mongo_bulk.py` — bulk ingest using `bulk_write` (InsertOne or ReplaceOne upsert), supports --dry-run and --upsert
- `tests/` — pytest tests for core helpers

- `examples/ingest_parquet_to_mongo_stream.py` — streaming ingest using ParquetFile.iter_batches(); supports concurrent bulk writes via `--max-workers`, `--batch-size`, `--upsert`, and `--dry-run`

Requirements

- Python 3.8+
- Install deps into a virtualenv (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Core examples and usage

1) Small demo example (in-memory)

```bash
python examples/run_example.py
```

2) Generate a large Parquet file with 50 fields (default 1,000,000 rows)

```bash
# default (1M rows) - may use a lot of RAM/disk
python examples/generate_big_parquet.py --rows 1000000 --out /tmp/big.parquet

# quick test (1k rows)
python examples/generate_big_parquet.py --rows 1000 --out /tmp/test_big.parquet
```

3) Generate Parquet from the DDL schema (streamed row-groups)

```bash
python examples/generate_parquet_from_ddl.py --rows 1000000 --out /tmp/ddl.parquet --chunk-size 100000

# quick test
python examples/generate_parquet_from_ddl.py --rows 1000 --out /tmp/ddl_test.parquet --chunk-size 500
```

4) Ingest Parquet -> MongoDB (safe dry-run first)

```bash
# dry-run (no DB writes)
python examples/ingest_parquet_to_mongo.py --parquet /tmp/ddl_test.parquet --dry-run

# real insert
export MONGO_URI='mongodb://user:pass@host:27017'
python examples/ingest_parquet_to_mongo.py --parquet /tmp/ddl.parquet --mongo-uri "$MONGO_URI" --db mydb --collection mycoll --batch-size 2000
```

5) Bulk ingest using `bulk_write` (higher throughput)

```bash
# dry-run
python examples/ingest_parquet_to_mongo_bulk.py --parquet /tmp/ddl_test.parquet --dry-run

# bulk insert
python examples/ingest_parquet_to_mongo_bulk.py --parquet /tmp/ddl.parquet --mongo-uri "$MONGO_URI" --db mydb --collection mycoll --batch-size 10000

# upsert mode (use composite PK fields):
python examples/ingest_parquet_to_mongo_bulk.py --parquet /tmp/ddl.parquet --mongo-uri "$MONGO_URI" --db mydb --collection mycoll --batch-size 5000 --upsert --pk-fields supplymethod_supplymethod,supplymethod_eff,supplymethod_transmode,item,location,supplymethod_sourcelocation
```

6) Streaming ingest (low memory) with parallel workers

The streaming ingest script processes an input Parquet file using PyArrow's
`ParquetFile.iter_batches()` which keeps peak memory usage low. When the target
MongoDB can handle concurrent writes, you can increase throughput by running
multiple concurrent `bulk_write` operations using the `--max-workers` flag.

```bash
# dry-run (inspect a single sample batch)
python examples/ingest_parquet_to_mongo_stream.py --parquet /tmp/ddl_test.parquet --dry-run

# real ingest with 8 parallel workers and 1k-op bulk batches
python examples/ingest_parquet_to_mongo_stream.py --parquet /tmp/ddl.parquet --mongo-uri "$MONGO_URI" --db mydb --collection mycoll --batch-size 1000 --max-workers 8

# upsert mode (use --pk-fields with a comma-separated list)
python examples/ingest_parquet_to_mongo_stream.py --parquet /tmp/ddl.parquet --mongo-uri "$MONGO_URI" --db mydb --collection mycoll --batch-size 1000 --max-workers 8 --upsert --pk-fields id_field,other_key
```

Notes & caveats

- Decimal handling: The DDL maps numeric(38,10) to Arrow decimal128(38,10). Generated values and ingestion convert decimals to `bson.decimal128.Decimal128` when `bson` is available; otherwise they fall back to string.
- Timestamps: timezone-aware timestamps are converted to naive UTC datetimes for MongoDB insertion (PyMongo stores naive datetimes in UTC by convention). Adjust if you need to preserve tzinfo.
- Memory: Generating 1M rows with many columns can use significant memory. Use `--chunk-size` or lower `--rows` if you run out of RAM. The DDL generator streams row-groups to minimize peak memory.
- Idempotency: Bulk/insert examples do plain inserts by default. Use `--upsert` and `--pk-fields` with the bulk ingest example to perform ReplaceOne upserts keyed by the provided fields.

- Concurrency and backpressure: The streaming script's `--max-workers` controls how many
	concurrent `bulk_write` calls are in-flight. Higher values can improve throughput but
	increase the number of outstanding batches held in memory. If you expect transient
	network or server errors, consider adding retries with backoff around failed bulk writes
	(not enabled by default). If you want an upper bound on memory used by queued work,
	keep `--max-workers` modest (4-8) and tune `--batch-size` accordingly.

Running tests

```bash
pytest -q
```

Next recommended improvements

- Stream Parquet reads using `ParquetFile.iter_batches()` to reduce memory pressure further when ingesting huge files.
- Add a small GitHub Actions workflow to run tests and linting.
- Add an option to partition Parquet outputs by a chosen column to test ingestion performance.

License: MIT
