import tempfile
from pathlib import Path

import pyarrow as pa

from parquet_demo import io


def test_write_and_read_roundtrip(tmp_path):
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
    ])

    rows = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'},
    ]

    path = tmp_path / 't.parquet'
    io.write_table(str(path), schema, rows)

    table = io.read_table(str(path))
    df = table.to_pandas()
    assert list(df['id']) == [1, 2]
    assert list(df['name']) == ['Alice', 'Bob']
