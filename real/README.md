# The real track — actual Apache Iceberg

The top-level `write.py` / `query.py` / `index.html` are a hand-rolled **toy**
that models Iceberg's mechanism so you can read every line. This folder is the
opposite: it drives the **official Apache implementation, [PyIceberg](https://py.iceberg.apache.org/)**,
to build **genuine Iceberg tables** on your local disk.

No cloud, no S3 — just a local SQLite catalog and a filesystem warehouse.

## Run it

```bash
pip install -r real/requirements.txt      # pyiceberg[pyarrow,sql-sqlite]
python real/iceberg_demo.py
```

## What it does (same movies as the toy, but real)

- Creates a real Iceberg table `movies.films`, **partitioned by genre**
  (hidden partitioning), tracked in a SQLite catalog.
- Appends the four movie batches as **four commits → four snapshots**, writing
  real **Parquet** data files and **Avro** manifests.
- Runs queries and shows the engine's own pruning:
  - `title == 'Margin Call'` → **min/max pruning**: reads **1 of 11** files.
  - `title LIKE 'I%'` → PyIceberg's `StartsWith` (a prefix *is* a range) →
    still prunes. (There is no "ends-with" expression — same lesson as the toy:
    min/max can't prune a leading wildcard.)
  - `genre == 'SciFi'` → **partition pruning**: skips whole partitions.
- **Time travel**: scans an older snapshot and shows `Margin Call` absent
  before the commit that added it.
- **Row-level delete**: `DELETE WHERE title == 'Drive'` creates a new snapshot;
  older snapshots still see the row.

## What gets written to disk

After a run, look in `real/warehouse/movies/films/`:

```
data/genre=Drama/*.parquet        real Parquet data files, laid out by partition
data/genre=SciFi/*.parquet
metadata/*.avro                   Avro manifests (…-m0.avro) + manifest lists (snap-*.avro)
metadata/*.metadata.json          the versioned table metadata (one per commit)
```

This is a bona fide Iceberg table — the same files Spark, Trino, or DuckDB
could open. We just happen to be driving it from Python.

## How this maps to the toy

| Concept | Toy (top level) | Real (here) |
|---|---|---|
| Data file | `data/file_N.csv` | `data/genre=…/*.parquet` |
| Manifest | `manifests/manifest_N.json` | `metadata/*-m0.avro` |
| Manifest list | `manifests/manifest_list.json` | `metadata/snap-*.avro` |
| Table pointer / metadata | (implicit) | `metadata/*.metadata.json` + SQLite catalog |
| min/max pruning | `_can_skip` in `query.py` | done by the engine's scan planner |
| Partition pruning | *(not in the toy)* | identity partition on `genre` |
| Time travel | `snapshot_id=` in `query.py` | `table.scan(snapshot_id=…)` |
| Row-level delete | *(not in the toy)* | `table.delete(...)` |

> Generated output (`real/warehouse/`, `real/catalog.db`) is git-ignored —
> it's rebuilt every run.
