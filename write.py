"""
write.py — a friendly quickstart wrapper over the `iceberg` engine.
===================================================================

The real work lives in the `iceberg/` package. This file is just the simplest
possible front door: one function, `write_batch(rows)`, that appends a batch of
movie dicts to a local Iceberg table (creating it on first use).

    from write import write_batch
    write_batch([{"title": "Tenet", "year": 2020, "genre": "SciFi"}])

Every call creates a new snapshot — see query.py to read it back with pruning,
and demo.py for the full tour (deletes, updates, compaction, time travel, ...).
"""

from iceberg import Catalog, Schema, Column, PartitionSpec, PartitionField

WAREHOUSE = "warehouse"          # local directory that holds the table
TABLE = "movies.films"           # namespace.table

# The table's columns, and partitioning by genre (identity transform) so that
# filtering on genre prunes whole partitions.
SCHEMA = Schema([
    Column(1, "title", "string"),
    Column(2, "year", "int"),
    Column(3, "genre", "string"),
])
PARTITION = PartitionSpec([PartitionField("genre", "identity")])


def table():
    """Load the movies table, creating it (and the warehouse) on first use."""
    cat = Catalog(WAREHOUSE)
    if cat.table_exists(TABLE):
        return cat.load_table(TABLE)
    return cat.create_table(TABLE, SCHEMA, PARTITION)


def write_batch(rows):
    """Append a batch of row dicts as a new snapshot; print a short summary."""
    t = table()
    snap = t.append(rows)
    s = snap["summary"]
    print(f"WROTE snapshot {snap['snapshot-id']}: +{s['added-records']} rows "
          f"in {s['added-data-files']} file(s), {s['total-records']} total")
    return snap


if __name__ == "__main__":
    # `python write.py` seeds a couple of rows as a smoke test.
    write_batch([
        {"title": "Argo", "year": 2012, "genre": "Thriller"},
        {"title": "Drive", "year": 2011, "genre": "Crime"},
    ])
