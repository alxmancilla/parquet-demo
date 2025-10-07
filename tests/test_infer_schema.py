from parquet_demo import io

from examples import run_example
import pyarrow as pa


def test_infer_schema_from_docs():
    docs = [
        {'id': 1, 'name': 'Alice', 'score': 9.5},
        {'id': 2, 'name': 'Bob', 'score': 7.3},
    ]

    schema = run_example.infer_schema_from_docs(docs)
    assert isinstance(schema, pa.Schema)
    names = [f.name for f in schema]
    assert 'id' in names and 'name' in names and 'score' in names
