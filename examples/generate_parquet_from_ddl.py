"""Generate a Parquet file with the schema described in the provided DDL.

This script streams row-groups to avoid building a single giant table in memory.
It maps PostgreSQL types to Arrow types and generates synthetic but reasonable
values for each column.

Usage:
  python examples/generate_parquet_from_ddl.py --rows 1000000 --out /tmp/out.parquet

Defaults are tuned to produce 1_000_000 rows in 10 row-groups by default.
"""
from __future__ import annotations

import argparse
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
import math
import random
from typing import Dict, Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def build_schema() -> pa.Schema:
    # Map DDL columns to Arrow types
    return pa.schema([
        pa.field('supplymethod_prodgroup', pa.string()),
        pa.field('supplymethod_splitfactor', pa.decimal128(38, 10)),
        pa.field('supplymethod_loadopt', pa.int64()),
        pa.field('supplymethod_convenientroundingprofile', pa.string()),
        pa.field('supplymethod_supplymethod', pa.string()),
        pa.field('supplymethod_yield', pa.float32()),
        pa.field('supplymethod_substdrawqty', pa.decimal128(38, 10)),
        pa.field('supplymethod_arrivalprofile', pa.string()),
        pa.field('supplymethod_prodfamily', pa.string()),
        pa.field('supplymethod_localbuildaheadlimit', pa.int64()),
        pa.field('supplymethod_eff', pa.timestamp('us', tz='UTC')),
        pa.field('supplymethod_transmode', pa.string()),
        pa.field('supplymethod_reviewcal', pa.string()),
        pa.field('supplymethod_unitexpeditecost', pa.decimal128(38, 10)),
        pa.field('supplymethod_maxfindur', pa.int64()),
        pa.field('supplymethod_maxstartdur', pa.int64()),
        pa.field('supplymethod_ordersubgroupid', pa.string()),
        pa.field('supplymethod_campaignminqty', pa.decimal128(38, 10)),
        pa.field('item', pa.string()),
        pa.field('supplymethod_delayprob', pa.float32()),
        pa.field('supplymethod_minqty', pa.decimal128(38, 10)),
        pa.field('supplymethod_everybucketloadsw', pa.bool_()),
        pa.field('supplymethod_campaignpriority', pa.decimal128(38, 10)),
        pa.field('supplymethod_ordergroupid', pa.string()),
        pa.field('supplymethod_yieldprofile', pa.string()),
        pa.field('supplymethod_disc', pa.timestamp('us')),
        pa.field('supplymethod_loaddur', pa.int64()),
        pa.field('location', pa.string()),
        pa.field('supplymethod_leadtime', pa.decimal128(38, 10)),
        pa.field('supplymethod_priority', pa.decimal128(38, 10)),
        pa.field('supplymethod_sourcelocation', pa.string()),
        pa.field('supplymethod_incqty', pa.decimal128(38, 10)),
        pa.field('supplymethod_nonewsupplydate', pa.timestamp('us')),
        pa.field('supplymethod_dyndepsrccost', pa.float32()),
        pa.field('supplymethod_leadtimevariance', pa.decimal128(38, 10)),
        pa.field('supplymethod_enabledyndepsw', pa.bool_()),
        pa.field('supplymethod_roundingfactor', pa.float32()),
        pa.field('supplymethod_replendur', pa.int64()),
        pa.field('supplymethod_unloaddur', pa.int64()),
        pa.field('supplymethod_type', pa.string()),
        pa.field('supplymethod_pushpriority', pa.int64()),
        pa.field('supplymethod_unitsupplymethodcost', pa.decimal128(38, 10)),
        pa.field('supplymethod_shippingprofile', pa.string()),
        pa.field('supplymethod_transcost', pa.float32()),
        pa.field('supplymethod_supplycapacityprofile', pa.string()),
        pa.field('supplymethod_altsrcpenalty', pa.float32()),
        pa.field('supplymethod_pushfactor', pa.float32()),
        pa.field('supplymethod_procurementcalendarid', pa.string()),
        pa.field('supplymethod_leadtimeeffncyprofile', pa.string()),
        pa.field('supplymethod_lastcampaignstartsw', pa.bool_()),
        pa.field('supplymethod_bomid', pa.string()),
        pa.field('ip__user_id', pa.string()),
        pa.field('ip__scenario_id', pa.string()),
    ])


def _gen_strings(n: int, prefix: str, card: int = 1000) -> np.ndarray:
    idx = np.arange(n, dtype=np.int64) % card
    return np.char.add(prefix + '_', idx.astype(str))


def _gen_decimal_ints(n: int, scale: int, low: int = -10**6, high: int = 10**6, seed: int | None = None):
    rs = np.random.RandomState(seed)
    # generate int base and scale upward
    arr = rs.randint(low, high, size=n, dtype=np.int64)
    # convert to Python ints and scale by 10**scale to represent decimal
    mul = 10 ** scale
    return [int(x) * mul for x in arr]


def _gen_timestamps_us(n: int, start: datetime, step_seconds: int = 1) -> list[datetime]:
    # produce timezone-aware UTC datetimes
    out = []
    for i in range(n):
        # keep values cycling so they don't grow too large
        dt = start + timedelta(seconds=(i % (365 * 24 * 3600)) * step_seconds)
        out.append(dt.replace(tzinfo=timezone.utc))
    return out


def write_parquet(path: Path, rows: int, chunk_size: int = 100000, seed: int | None = None):
    schema = build_schema()
    writer = pq.ParquetWriter(path.as_posix(), schema)

    start_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    start_nts = datetime(2020, 1, 1)
    rng = np.random.RandomState(seed)

    written = 0
    while written < rows:
        this_n = min(chunk_size, rows - written)

        # generate per-column arrays
        data: Dict[str, Any] = {}

        data['supplymethod_prodgroup'] = _gen_strings(this_n, 'prodgrp', card=500)
        data['supplymethod_splitfactor'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_loadopt'] = rng.randint(0, 1000000, size=this_n).astype(np.int64)
        data['supplymethod_convenientroundingprofile'] = _gen_strings(this_n, 'roundprof', card=200)
        # non-null primary key col
        data['supplymethod_supplymethod'] = _gen_strings(this_n, 'supplymethod', card=2000)
        data['supplymethod_yield'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_substdrawqty'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_arrivalprofile'] = _gen_strings(this_n, 'arrival', card=200)
        data['supplymethod_prodfamily'] = _gen_strings(this_n, 'prodfam', card=500)
        data['supplymethod_localbuildaheadlimit'] = rng.randint(0, 10000, size=this_n).astype(np.int64)
        data['supplymethod_eff'] = _gen_timestamps_us(this_n, start_ts)
        data['supplymethod_transmode'] = _gen_strings(this_n, 'trans', card=5)
        data['supplymethod_reviewcal'] = _gen_strings(this_n, 'revcal', card=50)
        data['supplymethod_unitexpeditecost'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_maxfindur'] = rng.randint(0, 100000, size=this_n).astype(np.int64)
        data['supplymethod_maxstartdur'] = rng.randint(0, 100000, size=this_n).astype(np.int64)
        data['supplymethod_ordersubgroupid'] = _gen_strings(this_n, 'ordersub', card=200)
        data['supplymethod_campaignminqty'] = _gen_decimal_ints(this_n, 10, seed=seed)
        # non-null primary key col
        data['item'] = _gen_strings(this_n, 'item', card=5000)
        data['supplymethod_delayprob'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_minqty'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_everybucketloadsw'] = (rng.randint(0, 2, size=this_n) == 0)
        data['supplymethod_campaignpriority'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_ordergroupid'] = _gen_strings(this_n, 'ordergrp', card=500)
        data['supplymethod_yieldprofile'] = _gen_strings(this_n, 'yieldprof', card=200)
        # timestamp without tz
        nt_dates = np.arange(this_n) % (365 * 24 * 3600)
        data['supplymethod_disc'] = (np.datetime64('2020-01-01') + nt_dates.astype('timedelta64[s]')).astype('datetime64[us]')
        data['supplymethod_loaddur'] = rng.randint(0, 100000, size=this_n).astype(np.int64)
        # non-null primary key col
        data['location'] = _gen_strings(this_n, 'loc', card=1000)
        data['supplymethod_leadtime'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_priority'] = _gen_decimal_ints(this_n, 10, seed=seed)
        # non-null primary key col
        data['supplymethod_sourcelocation'] = _gen_strings(this_n, 'sourceloc', card=1000)
        data['supplymethod_incqty'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_nonewsupplydate'] = (np.datetime64('2020-06-01') + (np.arange(this_n) % 365).astype('timedelta64[D]')).astype('datetime64[us]')
        data['supplymethod_dyndepsrccost'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_leadtimevariance'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_enabledyndepsw'] = (rng.randint(0, 2, size=this_n) == 0)
        data['supplymethod_roundingfactor'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_replendur'] = rng.randint(0, 100000, size=this_n).astype(np.int64)
        data['supplymethod_unloaddur'] = rng.randint(0, 100000, size=this_n).astype(np.int64)
        data['supplymethod_type'] = _gen_strings(this_n, 'type', card=20)
        data['supplymethod_pushpriority'] = rng.randint(0, 100000, size=this_n).astype(np.int64)
        data['supplymethod_unitsupplymethodcost'] = _gen_decimal_ints(this_n, 10, seed=seed)
        data['supplymethod_shippingprofile'] = _gen_strings(this_n, 'shipprof', card=200)
        data['supplymethod_transcost'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_supplycapacityprofile'] = _gen_strings(this_n, 'capprof', card=200)
        data['supplymethod_altsrcpenalty'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_pushfactor'] = rng.random_sample(this_n).astype(np.float32)
        data['supplymethod_procurementcalendarid'] = _gen_strings(this_n, 'procid', card=50)
        data['supplymethod_leadtimeeffncyprofile'] = _gen_strings(this_n, 'effprof', card=50)
        data['supplymethod_lastcampaignstartsw'] = (rng.randint(0, 2, size=this_n) == 0)
        data['supplymethod_bomid'] = _gen_strings(this_n, 'bomid', card=200)
        data['ip__user_id'] = _gen_strings(this_n, 'user', card=1000)
        data['ip__scenario_id'] = _gen_strings(this_n, 'scen', card=1000)

        # Convert arrays into pyarrow arrays honoring the schema
        pa_batch = {}
        for field in schema:
            name = field.name
            typ = field.type
            val = data[name]
            if pa.types.is_decimal(typ):
                # val is a list of Python ints scaled by 10**scale
                pa_batch[name] = pa.array(val, type=typ)
            elif pa.types.is_timestamp(typ) and typ.tz is not None:
                # supplymethod_eff: list of timezone-aware datetimes
                pa_batch[name] = pa.array(val, type=typ)
            else:
                pa_batch[name] = pa.array(val, type=typ)

        table = pa.table(pa_batch, schema=schema)
        writer.write_table(table)

        written += this_n
        print(f"Wrote {written}/{rows} rows")

    writer.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--out', type=Path, default=Path('ddl_output.parquet'))
    parser.add_argument('--chunk-size', type=int, default=100_000)
    parser.add_argument('--seed', type=int, default=0)
    args = parser.parse_args(argv)

    write_parquet(args.out, args.rows, chunk_size=args.chunk_size, seed=args.seed)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
