"""Generate a large Parquet file for benchmarking/demo.

Produces a Parquet file with 50 fields and a configurable number of rows
(default 1_000_000). This example uses numpy for fast in-memory array
generation and pyarrow to write the Parquet file.

Usage examples:
  python examples/generate_big_parquet.py --rows 1000000 --out /tmp/big.parquet
  python -m examples.generate_big_parquet --rows 100000 --out ./big.parquet

Notes:
- Generating 1M x 50 fields will use significant memory. Consider lowering
  --rows or using row-grouping/streaming if you run out of memory.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import time
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def build_table(num_rows: int, seed: int | None = 0) -> pa.Table:
    rs = np.random.RandomState(seed)
    n = num_rows

    # Field counts to total 50
    ints_count = 12
    floats_count = 12
    strs_count = 12
    ts_count = 6
    date_count = 4
    bool_count = 4

    arrays = {}

    # Integers: sequential ranges with small offsets
    for i in range(ints_count):
        arrays[f"i_{i}"] = (np.arange(n, dtype=np.int64) + i)

    # Floats: random floats
    for i in range(floats_count):
        arrays[f"f_{i}"] = rs.random_sample(n).astype(np.float64)

    # Strings: repeatable pattern to avoid massive unique strings
    base_idx = np.arange(n, dtype=np.int64)
    # keep the string cardinality limited to reduce memory and file size
    card = max(1_000, n // 1000)
    s_idx = (base_idx % card).astype(str)
    for i in range(strs_count):
        # use np.char.add which is stable across numpy versions
        arrays[f"s_{i}"] = np.char.add("str_", s_idx)

    # Timestamps: datetime64[us] values derived from a base
    base_ts = np.datetime64("2020-01-01T00:00:00", "s")
    sec_offsets = (base_idx % (365 * 24 * 3600)).astype("timedelta64[s]")
    for i in range(ts_count):
        # make slight offsets per column so values differ
        arr = (base_ts + sec_offsets + np.timedelta64(i, "s")).astype("datetime64[us]")
        arrays[f"ts_{i}"] = arr

    # Dates: date64 or datetime64[D]
    base_date = np.datetime64("2020-01-01", "D")
    day_offsets = (base_idx % 365).astype("timedelta64[D]")
    for i in range(date_count):
        arrays[f"d_{i}"] = (base_date + day_offsets).astype("datetime64[D]")

    # Bools
    for i in range(bool_count):
        arrays[f"b_{i}"] = (base_idx % 2 == (i % 2))

    # Build a pyarrow Table; pyarrow handles numpy datetime64 and strings.
    return pa.table(arrays)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a big Parquet file (50 fields)")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate")
    parser.add_argument("--out", type=Path, default=Path("big.parquet"), help="Output parquet path")
    parser.add_argument("--seed", type=int, default=0, help="Random seed")
    parser.add_argument("--row-group-size", type=int, default=100_000, help="Parquet row-group size")
    args = parser.parse_args(argv)

    print(f"Generating {args.rows:,} rows with 50 fields...")
    t0 = time.time()
    table = build_table(args.rows, seed=args.seed)
    t1 = time.time()
    print(f"Built table in {t1 - t0:.2f}s — approx memory: {table.nbytes / (1024**2):.1f} MiB")

    out = args.out
    print(f"Writing to {out} (row_group_size={args.row_group_size})...")
    pq.write_table(table, out.as_posix(), row_group_size=args.row_group_size)
    t2 = time.time()
    print(f"Wrote parquet in {t2 - t1:.2f}s — total elapsed {t2 - t0:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
