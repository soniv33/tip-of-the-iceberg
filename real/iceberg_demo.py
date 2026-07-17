"""
real/iceberg_demo.py — the SAME demo, but on REAL Apache Iceberg.
====================================================================

The rest of this repo (write.py / query.py / index.html) is a hand-rolled toy
that models Iceberg's *mechanism* so you can read every line. This file does
the opposite: it uses the **official Apache implementation, PyIceberg**, to
build genuine Iceberg tables on your local disk.

What "real" buys you here:
    * Actual Parquet data files and Avro manifests / manifest lists, plus a
      versioned metadata.json — the real on-disk Iceberg spec, not JSON+CSV.
    * A real catalog (SQLite) tracking the table's current metadata pointer.
    * Real min/max pruning AND partition pruning done by the engine, not us.
    * Real snapshots, time travel, and row-level deletes.
    * Files that Spark / Trino / DuckDB could open — this is a bona fide
      Iceberg table, we just happen to be driving it from Python.

Everything is local: a SQLite catalog + a filesystem "warehouse" directory.
No cloud, no S3.

Run:
    pip install "pyiceberg[pyarrow,sql-sqlite]"
    python real/iceberg_demo.py
"""

import os
import shutil

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo, StartsWith, GreaterThanOrEqual

# ---------------------------------------------------------------------------
# Local layout. The "warehouse" holds the actual table data + metadata; the
# catalog.db is a SQLite file that remembers where each table's current
# metadata.json lives. Both are just files on disk.
# ---------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(HERE, "warehouse")
CATALOG_DB = os.path.join(HERE, "catalog.db")
TABLE_ID = "movies.films"

# The same movie batches as demo.py, so you can compare the toy to the real
# thing directly. Each batch is appended as its own commit -> its own snapshot.
BATCHES = [
    [
        {"title": "Amelie", "year": 2001, "genre": "Romance"},
        {"title": "Argo", "year": 2012, "genre": "Thriller"},
        {"title": "Boyhood", "year": 2014, "genre": "Drama"},
        {"title": "Drive", "year": 2011, "genre": "Crime"},
    ],
    [
        {"title": "Fargo", "year": 1996, "genre": "Crime"},
        {"title": "Gattaca", "year": 1997, "genre": "SciFi"},
        {"title": "Gravity", "year": 2013, "genre": "SciFi"},
        {"title": "Her", "year": 2013, "genre": "Romance"},
    ],
    [
        {"title": "Inception", "year": 2010, "genre": "SciFi"},
        {"title": "Interstellar", "year": 2014, "genre": "SciFi"},
        {"title": "Magnolia", "year": 1999, "genre": "Drama"},
        {"title": "Margin Call", "year": 2011, "genre": "Drama"},
        {"title": "Moonlight", "year": 2016, "genre": "Drama"},
    ],
    [
        {"title": "Nightcrawler", "year": 2014, "genre": "Thriller"},
        {"title": "Prisoners", "year": 2013, "genre": "Thriller"},
        {"title": "Sicario", "year": 2015, "genre": "Thriller"},
        {"title": "Whiplash", "year": 2014, "genre": "Drama"},
        {"title": "Zodiac", "year": 2007, "genre": "Thriller"},
    ],
]

# The Iceberg schema. Field IDs are explicit because Iceberg tracks columns by
# ID (that's how it does safe schema evolution — renames don't lose data).
SCHEMA = Schema(
    NestedField(1, "title", StringType(), required=False),
    NestedField(2, "year", IntegerType(), required=False),
    NestedField(3, "genre", StringType(), required=False),
)

# PyArrow schema used to build the record batches we append. Types must line
# up with the Iceberg schema (Iceberg IntegerType == 32-bit int).
PA_SCHEMA = pa.schema(
    [
        pa.field("title", pa.string(), nullable=True),
        pa.field("year", pa.int32(), nullable=True),
        pa.field("genre", pa.string(), nullable=True),
    ]
)

# Partition the table by genre (identity transform). This is "hidden
# partitioning": queries filter on `genre` normally and Iceberg prunes whole
# partitions for us — no partition columns in the query, no directory tricks.
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="genre")
)


def _reset():
    """Wipe any previous run so the demo is reproducible."""
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    if os.path.exists(CATALOG_DB):
        os.remove(CATALOG_DB)
    os.makedirs(WAREHOUSE, exist_ok=True)


def _catalog():
    """Open (creating on first use) a local SQLite-backed Iceberg catalog."""
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{CATALOG_DB}",
        warehouse=f"file://{WAREHOUSE}",
    )


def _rule(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _short(file_path):
    """Iceberg stores data-file paths as file:// URIs; show them warehouse-relative."""
    path = file_path.replace("file://", "")
    return os.path.relpath(path, WAREHOUSE)


def main():
    _reset()
    catalog = _catalog()
    catalog.create_namespace_if_not_exists("movies")

    # --- Create the real Iceberg table --------------------------------
    _rule("CREATE TABLE (real Iceberg: schema + partition spec + catalog)")
    table = catalog.create_table(
        TABLE_ID, schema=SCHEMA, partition_spec=PARTITION_SPEC
    )
    print(f"Created {TABLE_ID}, partitioned by genre.")
    print(f"Metadata location: {table.metadata_location}")

    # --- Append the four batches as four commits ----------------------
    _rule("APPEND four batches -> four snapshots (real Parquet + Avro)")
    for i, batch in enumerate(BATCHES, start=1):
        table.append(pa.Table.from_pylist(batch, schema=PA_SCHEMA))
        table.refresh()
        snap = table.current_snapshot()
        print(f"  commit {i}: snapshot_id={snap.snapshot_id}  "
              f"({snap.summary.get('added-data-files', '?')} data files added, "
              f"{snap.summary.get('total-records', '?')} total records)")

    # --- Show the real snapshot history -------------------------------
    _rule("SNAPSHOT HISTORY (from the table metadata)")
    for snap in table.snapshots():
        print(f"  snapshot_id={snap.snapshot_id}  op={snap.summary.get('operation')}  "
              f"records={snap.summary.get('total-records')}  files={snap.summary.get('total-data-files')}")

    # --- min/max pruning: engine skips files by column stats ----------
    _rule("QUERY  title == 'Margin Call'  (min/max file pruning by the engine)")
    total_files = len(list(table.scan().plan_files()))
    tasks = list(table.scan(row_filter=EqualTo("title", "Margin Call")).plan_files())
    print(f"  Files in table: {total_files}. Files the planner will read: {len(tasks)}.")
    for t in tasks:
        print(f"    -> {_short(t.file.file_path)}")
    rows = table.scan(row_filter=EqualTo("title", "Margin Call")).to_arrow().to_pylist()
    print(f"  Result: {rows}")

    # --- prefix LIKE = StartsWith, still prunable ---------------------
    _rule("QUERY  title LIKE 'I%'  (StartsWith — a prefix is a range, still prunes)")
    tasks = list(table.scan(row_filter=StartsWith("title", "I")).plan_files())
    print(f"  Files the planner will read: {len(tasks)} of {total_files}.")
    rows = table.scan(row_filter=StartsWith("title", "I")).to_arrow().to_pylist()
    print(f"  Result: {[r['title'] for r in rows]}")

    # --- partition pruning: filter on genre ---------------------------
    _rule("QUERY  genre == 'SciFi'  (PARTITION pruning — whole partitions skipped)")
    tasks = list(table.scan(row_filter=EqualTo("genre", "SciFi")).plan_files())
    print(f"  Files the planner will read: {len(tasks)} of {total_files} "
          f"(only the SciFi partition).")
    rows = table.scan(row_filter=EqualTo("genre", "SciFi")).to_arrow().to_pylist()
    print(f"  Result: {[r['title'] for r in rows]}")

    # --- time travel: scan an older snapshot --------------------------
    _rule("TIME TRAVEL  (scan the table as it was after commit #2)")
    second_snapshot = table.snapshots()[1].snapshot_id
    tt = table.scan(snapshot_id=second_snapshot,
                    row_filter=EqualTo("title", "Margin Call")).to_arrow().to_pylist()
    print(f"  At snapshot {second_snapshot}, 'Margin Call' rows: {tt}  "
          f"(empty — it wasn't written until commit #3)")

    # --- row-level delete: a real Iceberg delete ----------------------
    _rule("DELETE  where title == 'Drive'  (row-level delete = new snapshot)")
    table.delete(EqualTo("title", "Drive"))
    table.refresh()
    remaining = table.scan(row_filter=EqualTo("title", "Drive")).to_arrow().to_pylist()
    print(f"  'Drive' after delete (latest snapshot): {remaining}  (gone)")
    print(f"  Snapshots now: {len(table.snapshots())} "
          f"(the delete added one; older snapshots still see 'Drive').")

    # --- show the real on-disk files ----------------------------------
    _rule("ON DISK  (this is a genuine Iceberg table layout)")
    counts = {}
    for root, _dirs, files in os.walk(WAREHOUSE):
        for f in files:
            ext = ".metadata.json" if f.endswith(".metadata.json") else os.path.splitext(f)[1]
            counts[ext] = counts.get(ext, 0) + 1
    for ext, n in sorted(counts.items()):
        label = {
            ".parquet": "Parquet data files",
            ".avro": "Avro manifests / manifest lists",
            ".metadata.json": "table metadata versions",
        }.get(ext, ext)
        print(f"  {n:>3}  {label}")
    print(f"\n  Warehouse root: {WAREHOUSE}")
    print("  Explore real/warehouse/movies/films/  — data/ holds Parquet "
          "(partitioned by genre), metadata/ holds the Avro manifests + metadata.json.")


if __name__ == "__main__":
    main()
