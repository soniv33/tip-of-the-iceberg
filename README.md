# tip-of-the-iceberg

A compact, **from-scratch implementation of Apache Iceberg's table format and
mechanism**, in pure Python (standard library only — no pyarrow, no cloud, no
external dependencies).

This is a *working engine*, not a mock. It maintains a real Iceberg-style
metadata tree, prunes files by statistics at read time, and supports snapshots,
time travel, copy-on-write **and** merge-on-read deletes, updates, upserts,
compaction, snapshot expiration, and schema evolution.

It is deliberately **readable rather than byte-compatible with the spec**: data
files are CSV instead of Parquet and metadata is JSON instead of Avro, so you
can open every file in the warehouse and see exactly what the engine did. The
concepts and control flow are the real ones — this is not the official
[Apache Iceberg](https://iceberg.apache.org/) library, and it isn't meant for
production, but it implements the same ideas end to end.

```bash
python demo.py     # the full guided tour (no install needed)
```

There's also an [`index.html`](index.html) — a self-contained, phone-friendly
**interactive companion** that ports the whole engine to the browser: query
with live SKIP/OPEN pruning, append/delete (copy-on-write or merge-on-read)/
update/upsert/compact/expire/add-column, a live view of the snapshot timeline
and files-by-partition, click-to-time-travel, and a narration log explaining
what Iceberg did at each step. No build, no server — open the file.

---

## What it implements

| Capability | Where | Notes |
|---|---|---|
| Schema & typed columns | `iceberg/schema.py` | field IDs, CSV (de)serialization |
| Partitioning | `iceberg/schema.py` | identity / bucket[N] / truncate[W] transforms |
| Per-file statistics | `iceberg/schema.py` | record count, null counts, min/max bounds |
| Predicate pushdown | `iceberg/expressions.py` | `==,!=,<,<=,>,>=, IN, IS NULL, LIKE 'x%'`, `AND/OR/NOT` |
| Metadata tree | `iceberg/table.py` | metadata.json → manifest list → manifests → data files |
| Snapshots & time travel | `iceberg/table.py` | by `snapshot_id` or as-of timestamp |
| Append (incremental) | `iceberg/table.py` | reuses parent manifests, writes one new manifest |
| Delete — copy-on-write | `iceberg/table.py` | rewrites data files without the rows |
| Delete — merge-on-read | `iceberg/table.py` | writes equality **delete files**, applied at read via sequence numbers |
| Update / Upsert (MERGE) | `iceberg/table.py` | delete + insert as one snapshot |
| Compaction | `iceberg/table.py` | merges small files per partition, folds in delete files |
| Snapshot expiration + GC | `iceberg/table.py` | drops old snapshots, garbage-collects unreferenced files |
| Schema evolution | `iceberg/table.py` | add a column without rewriting existing data |

---

## The core idea (pruning)

A naive engine answering `title == 'Margin Call'` opens every file and scans
every row. Iceberg records the **min and max of each column in each file** at
write time, so at read time it can *prove* a file can't contain your value and
**skip it unopened**:

```
scan title == 'Margin Call'  (snapshot …, 11 data file(s))
  SKIP  …Amelie.csv     (title [Amelie..Amelie] — stats rule it out)
  …
  OPEN  …Magnolia.csv   (title [Magnolia..Moonlight] — checking rows)
  …
  -> opened 1/11, 1 matching row(s)
```

The same statistics extend to ranges (`year >= 2014`), `IN`, and prefix `LIKE`
(`'Ma%'` is the range `['Ma','Mb')`). A leading-wildcard `LIKE '%all'` has no
bound, so it can't be pruned — that's the honest limit of min/max, and the
engine reflects it. Partitioning (e.g. by `genre`) makes pruning even sharper:
filtering on the partition column skips whole partitions.

## The metadata tree (and time travel)

Every write creates a new **snapshot** and never mutates the past:

```
metadata/vN.metadata.json     the table root (schema, spec, current snapshot)
        │
        ▼
metadata/snap-<id>.json        a snapshot's MANIFEST LIST
        │
        ▼
metadata/manifest-*.json       MANIFESTS (lists of data files + their stats)
        │
        ▼
data/genre=…/*.csv             the DATA FILES (CSV here; Parquet in real Iceberg)
```

Because snapshots are append-only, `scan(snapshot_id=…)` (or an as-of timestamp)
reconstructs exactly what the table looked like earlier — time travel. An
`update` or `delete` is never an in-place edit; it's a new snapshot whose files
supersede the old ones, while the old ones live on for time travel until you
`expire_snapshots`.

## Deletes: copy-on-write vs merge-on-read

Both are real Iceberg strategies, and both are here:

- **Copy-on-write** rewrites the affected data files without the deleted rows.
  Reads stay fast; writes are heavier.
- **Merge-on-read** writes a tiny **equality delete file** and leaves data
  files untouched; reads merge the deletes on the fly (an equality delete
  applies only to data files with a *lower sequence number*, which is why
  re-inserting a deleted key brings it back). **Compaction** later folds the
  delete files back into rewritten data.

---

## Use it yourself

```python
from iceberg import Catalog, Schema, Column, PartitionSpec, PartitionField
from iceberg import expressions as E

cat = Catalog("warehouse")
films = cat.create_table(
    "movies.films",
    Schema([Column(1, "title", "string"), Column(2, "year", "int"),
            Column(3, "genre", "string")]),
    PartitionSpec([PartitionField("genre", "identity")]),
)

films.append([{"title": "Margin Call", "year": 2011, "genre": "Drama"}])
films.scan(E.Eq("title", "Margin Call")).explain()      # prints the pruning trace
films.scan(E.GtEq("year", 2010)).rows()                 # -> list of row dicts
films.delete(E.Eq("title", "Margin Call"), mode="merge-on-read")
films.compact()
```

Or use the friendly one-liners: `write.py` (`write_batch(rows)`) and `query.py`
(`query(col, val, op=..., snapshot_id=...)`).

## Project layout

```
tip-of-the-iceberg/
  iceberg/            the engine
    schema.py         columns, types, partition transforms, file stats
    expressions.py    predicates: row evaluation + file pruning
    table.py          catalog, metadata tree, scans, all operations
  write.py            quickstart: write_batch(rows)
  query.py            quickstart: query(col, val, op, snapshot_id)
  demo.py             full guided tour of every feature
  index.html          interactive browser companion (visualizes pruning)
  warehouse/          tables land here (git-ignored; rebuilt on each run)
```

## How it maps to real Apache Iceberg

| This engine | Real Iceberg |
|---|---|
| CSV data files | Parquet/ORC/Avro data files |
| JSON manifests / manifest lists | Avro manifests / manifest lists |
| `catalog.json` + `version-hint.text` | a catalog (Hive, REST, Glue, Nessie, …) |
| whole-table rewrite on COW ops | rewrites only the affected files |
| one manifest per append | manifest merging / rewrite strategies |

The simplifications are about *format and scale*, not about the mechanism —
the snapshot model, metadata tree, pruning, deletes, and maintenance are the
real design.
