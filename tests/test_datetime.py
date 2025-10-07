import datetime as dt
from pathlib import Path

import pyarrow as pa

from parquet_demo import io


def test_write_and_read_datetime_roundtrip(tmp_path):
    schema = pa.schema([
        pa.field('ts', pa.timestamp('us')),
        pa.field('d', pa.date32()),
    ])

    rows = [
        {'ts': dt.datetime(2023, 1, 2, 3, 4, 5, 123456), 'd': dt.date(2023, 1, 2)},
        {'ts': dt.datetime(2024, 6, 7, 8, 9, 10), 'd': dt.date(2024, 6, 7)},
    ]

    path = tmp_path / 'dt.parquet'
    io.write_table(str(path), schema, rows)

    table = io.read_table(str(path))
    # to_pandas may return either Python date objects or datetimelike values;
    # accept both behaviors.
    df = table.to_pandas()
    d_vals = df['d'].tolist()
    if d_vals and isinstance(d_vals[0], dt.date) and not isinstance(d_vals[0], dt.datetime):
        assert d_vals == [dt.date(2023, 1, 2), dt.date(2024, 6, 7)]
    else:
        # pandas datetime-like
        assert df['d'].dt.date.tolist() == [dt.date(2023, 1, 2), dt.date(2024, 6, 7)]

    assert df['ts'].dt.year.tolist() == [2023, 2024]
