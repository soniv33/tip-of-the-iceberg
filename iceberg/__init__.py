"""
iceberg — a compact, from-scratch implementation of Apache Iceberg's table
format and read/write mechanism, in pure Python (standard library only).

This is a *working* engine, not a mock: it maintains a real metadata tree
(table metadata -> manifest lists -> manifests -> data files), does min/max +
partition pruning at scan time, and supports snapshots, time travel, copy-on-
write and merge-on-read deletes, updates, upserts, compaction, snapshot
expiration, and schema evolution.

It is deliberately readable rather than spec-byte-compatible: data files are
CSV instead of Parquet and metadata is JSON instead of Avro, so you can open
every file and see what the engine is doing. The concepts and control flow are
the real ones.

Quick start:

    from iceberg import Catalog, Schema, Column, PartitionSpec, PartitionField
    from iceberg import expressions as E

    cat = Catalog("warehouse")
    films = cat.create_table(
        "movies.films",
        Schema([Column(1, "title", "string"), Column(2, "year", "int"),
                Column(3, "genre", "string")]),
        PartitionSpec([PartitionField("genre", "identity")]),
    )
    films.append([{"title": "Argo", "year": 2012, "genre": "Thriller"}])
    films.scan(E.Eq("title", "Argo")).explain()
"""

from .schema import Schema, Column, PartitionSpec, PartitionField, compute_stats
from .table import Catalog, Table, Scan
from . import expressions

__all__ = [
    "Catalog", "Table", "Scan",
    "Schema", "Column", "PartitionSpec", "PartitionField", "compute_stats",
    "expressions",
]
