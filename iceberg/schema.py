"""
iceberg.schema — columns, types, partition transforms, and file statistics.
============================================================================

This module holds the "shape" pieces of the table:

    * Schema / Column     — the columns and their types.
    * PartitionSpec       — how rows are grouped into partitions (identity,
                            bucket[N], truncate[W]) — Iceberg's "hidden
                            partitioning".
    * compute_stats       — the per-file min/max/null-count statistics that
                            power pruning at read time.

Data files are stored as CSV (everything comes back as strings), so the schema
also knows how to cast a raw CSV row into properly typed Python values and how
to format typed values back out to strings.
"""

import hashlib

# Supported column types. int/long both map to Python int; double to float.
_TYPES = {"string", "int", "long", "double", "boolean"}


class Column:
    """One column: a stable field id, a name, a type, and nullability."""

    def __init__(self, field_id, name, type_, required=False):
        assert type_ in _TYPES, f"unknown type {type_}"
        self.field_id = field_id
        self.name = name
        self.type = type_
        self.required = required

    def to_dict(self):
        return {"id": self.field_id, "name": self.name,
                "type": self.type, "required": self.required}

    @staticmethod
    def from_dict(d):
        return Column(d["id"], d["name"], d["type"], d["required"])


def _cast(type_, s):
    """Turn a raw CSV string into a typed value (or None for empty)."""
    if s is None or s == "":
        return None
    if type_ == "string":
        return s
    if type_ in ("int", "long"):
        return int(s)
    if type_ == "double":
        return float(s)
    if type_ == "boolean":
        return s.lower() in ("true", "1", "t", "yes")
    raise ValueError(type_)


def _format(v):
    """Turn a typed value into a CSV string ('' for None)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


class Schema:
    """An ordered list of columns, plus type-aware row (de)serialization."""

    def __init__(self, columns):
        self.columns = list(columns)

    def names(self):
        return [c.name for c in self.columns]

    def column(self, name):
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)

    def last_column_id(self):
        return max((c.field_id for c in self.columns), default=0)

    def cast_row(self, raw):
        """CSV dict (str->str) -> typed dict. Missing columns become None
        (this is what makes reading files written before a column was added
        just work — the basis of safe schema evolution)."""
        return {c.name: _cast(c.type, raw.get(c.name)) for c in self.columns}

    def format_row(self, row):
        """Typed dict -> CSV-ready dict of strings."""
        return {c.name: _format(row.get(c.name)) for c in self.columns}

    def to_dict(self):
        return {"columns": [c.to_dict() for c in self.columns]}

    @staticmethod
    def from_dict(d):
        return Schema([Column.from_dict(c) for c in d["columns"]])


# ---------------------------------------------------------------------------
# Partition transforms. A transform maps a source column value to a partition
# value; rows that share a partition value are written to the same file/dir.
# ---------------------------------------------------------------------------
def _bucket(value, n):
    """Stable hash bucket — deterministic across runs (unlike hash())."""
    digest = hashlib.md5(str(value).encode()).hexdigest()
    return int(digest, 16) % n


def _truncate(value, width):
    if isinstance(value, str):
        return value[:width]
    return (value // width) * width  # numeric truncation


class PartitionField:
    """One partitioning rule: a transform applied to a source column."""

    def __init__(self, source, transform, name=None):
        # transform is "identity", "bucket[N]", or "truncate[W]".
        self.source = source
        self.transform = transform
        self.name = name or self._default_name()

    def _default_name(self):
        if self.transform == "identity":
            return self.source
        kind = self.transform.split("[")[0]
        return f"{self.source}_{kind}"

    def _param(self):
        return int(self.transform[self.transform.index("[") + 1: -1])

    def apply(self, value):
        if value is None:
            return None
        if self.transform == "identity":
            return value
        if self.transform.startswith("bucket"):
            return _bucket(value, self._param())
        if self.transform.startswith("truncate"):
            return _truncate(value, self._param())
        raise ValueError(self.transform)

    def to_dict(self):
        return {"source": self.source, "transform": self.transform, "name": self.name}

    @staticmethod
    def from_dict(d):
        return PartitionField(d["source"], d["transform"], d["name"])


class PartitionSpec:
    """A set of PartitionFields (possibly empty = unpartitioned table)."""

    def __init__(self, fields=None):
        self.fields = list(fields or [])

    def partition_for(self, row):
        """Return the partition value dict for a row, e.g. {'genre': 'SciFi'}."""
        return {f.name: f.apply(row.get(f.source)) for f in self.fields}

    def path_for(self, partition):
        """Directory path fragment, e.g. 'genre=SciFi' (Hive-style, familiar)."""
        if not self.fields:
            return ""
        return "/".join(f"{f.name}={partition[f.name]}" for f in self.fields)

    def columns(self):
        return {f.source for f in self.fields}

    def to_dict(self):
        return {"fields": [f.to_dict() for f in self.fields]}

    @staticmethod
    def from_dict(d):
        return PartitionSpec([PartitionField.from_dict(f) for f in d["fields"]])


def compute_stats(rows, schema):
    """
    Compute the per-file statistics Iceberg records in a manifest:

        record_count   — how many rows.
        null_counts    — per column, how many nulls.
        lower_bounds   — per column, the min non-null value.
        upper_bounds   — per column, the max non-null value.

    These are exactly what the scan planner uses to skip files it can prove
    contain no matching row.
    """
    record_count = len(rows)
    null_counts, lower, upper = {}, {}, {}
    for col in schema.names():
        values = [r.get(col) for r in rows]
        nulls = sum(1 for v in values if v is None)
        non_null = [v for v in values if v is not None]
        null_counts[col] = nulls
        if non_null:
            lower[col] = min(non_null)
            upper[col] = max(non_null)
    return {
        "record_count": record_count,
        "null_counts": null_counts,
        "lower_bounds": lower,
        "upper_bounds": upper,
    }
