from typing import Iterable
import datetime as _dt
import pyarrow as pa
import pyarrow.parquet as pq


def write_table(path: str, schema: pa.Schema, rows: Iterable[dict]):
    """Write rows (iterable of dict) to a Parquet file at `path` using the provided Arrow schema.

    Args:
        path: destination parquet file path
        schema: pyarrow.Schema describing the table
        rows: iterable of dict where keys match schema field names
    """
    # Convert iterable of dict to columns according to schema
    arrays = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            arrays[field.name].append(row.get(field.name))

    def _coerce_list(lst, field_type: pa.DataType):
        """Coerce Python objects in lst to values compatible with pa.array for field_type.

        - For timestamp types, convert datetime to integer microseconds since epoch.
        - For date32, convert date to days since epoch.
        - Leave None values untouched.
        - For other types, return as-is.
        """
        if pa.types.is_timestamp(field_type):
            # convert datetimes to integer microseconds
            unit = field_type.unit if hasattr(field_type, 'unit') else 'us'

            def to_ts(v):
                if v is None:
                    return None
                if isinstance(v, _dt.datetime):
                    # ensure naive datetimes are treated as UTC
                    if v.tzinfo is not None:
                        v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
                    epoch = _dt.datetime(1970, 1, 1)
                    delta = v - epoch
                    us = int(delta.total_seconds() * 1_000_000) + (v.microsecond)
                    if unit == 's':
                        return int(us // 1_000_000)
                    if unit == 'ms':
                        return int(us // 1_000)
                    # default to microseconds
                    return us
                # allow integers already
                return v

            return [to_ts(v) for v in lst]

        if pa.types.is_date32(field_type):
            def to_date32(v):
                if v is None:
                    return None
                if isinstance(v, _dt.date) and not isinstance(v, _dt.datetime):
                    epoch = _dt.date(1970, 1, 1)
                    return (v - epoch).days
                return v

            return [to_date32(v) for v in lst]

        # default: no coercion
        return lst

    pa_arrays = []
    for field in schema:
        raw = arrays[field.name]
        coerced = _coerce_list(raw, field.type)
        pa_arrays.append(pa.array(coerced, type=field.type))

    table = pa.Table.from_arrays(pa_arrays, schema=schema)
    pq.write_table(table, path)


def read_table(path: str) -> pa.Table:
    """Read a Parquet file and return a pyarrow.Table."""
    return pq.read_table(path)
