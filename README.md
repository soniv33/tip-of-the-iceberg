# tip-of-the-iceberg

A minimal, **educational** reimplementation of the one idea that makes
[Apache Iceberg](https://iceberg.apache.org/) fast: **metadata-driven file
pruning with snapshots for time travel.**

Local files only. No cloud, no S3, no Spark, no external dependencies — just
the Python standard library. Data files are CSV so you can `cat` them and see
exactly what's inside.

**📱 Try it in your browser / on your phone:** open [`index.html`](index.html) —
a self-contained page (no server, no build step) that mirrors the Python and
lets you run queries and watch the SKIP/OPEN pruning decisions interactively.
See [Live demo](#live-demo-phone--browser) below to host it on GitHub Pages.

> ⚠️ **This is a toy for learning, not a real implementation.** It exists to
> make one mechanism legible in ~300 lines of commented Python. Real Iceberg
> is far more capable (schema evolution, hidden partitioning, Avro/Parquet,
> concurrent writers, deletes, catalogs, and much more). Don't put this
> anywhere near production.

---

## The one idea

When you query a big table for `title == 'Margin Call'`, the slow way is to
open every data file and scan every row. Iceberg avoids this by **writing down
metadata at write time** — including the **min and max of every column in
every file** — so that at read time it can *prove* a file can't contain your
value and **skip it without ever opening it**.

If a file's titles run from `Amelie` to `Drive`, then `Margin Call` (which
sorts after `Drive`) simply cannot be in it. Skip it. That's min/max pruning,
and it's the whole trick.

---

## What this demonstrates

Run the demo:

```bash
python demo.py
```

You'll see four writes (each becomes a snapshot), then queries that print a
decision for every file:

```
QUERY  title == 'Margin Call'   (snapshot #4, 4 data file(s))
--------------------------------------------------------------------
  SKIP  file_1.csv     (range Amelie–Drive, doesn't contain 'Margin Call')
  SKIP  file_2.csv     (range Fargo–Her, doesn't contain 'Margin Call')
  OPEN  file_3.csv     (range Inception–Moonlight, checking rows)
          -> match: {'title': 'Margin Call', 'year': '2011', 'genre': 'Drama'}
  SKIP  file_4.csv     (range Nightcrawler–Zodiac, doesn't contain 'Margin Call')
--------------------------------------------------------------------
  Pruned 3/4 files without opening them. Found 1 matching row(s).
```

Three of four files are eliminated using metadata alone. Only the surviving
file is actually read.

---

## How it maps to real Apache Iceberg

Iceberg organizes metadata in a three-level tree. This project mirrors that
tree exactly, just with simpler file formats:

| Real Iceberg | This project | Role |
|---|---|---|
| **Manifest list** (one Avro file per snapshot) | `manifests/manifest_list.json` (grows, one entry per snapshot) | The table's history. Points to the manifests visible in each snapshot. |
| **Manifest** (Avro) | `manifests/manifest_N.json` | Describes a set of data files: their paths, row counts, and per-column min/max stats. |
| **Data file** (Parquet) | `data/file_N.csv` | The actual rows. |
| Per-column min/max in the manifest | `column_stats: {min, max}` | The statistics that drive pruning. |
| Snapshot | one entry in the `snapshots` list | An immutable point-in-time view of the table. |
| Time travel | `query(..., snapshot_id=N)` | Read an older snapshot to see the table as it was. |

The read path walks the tree top-down, exactly like Iceberg:

```
manifest_list.json      "which manifests exist in this snapshot?"
        │
        ▼
manifest_N.json         "which data files, and what are their min/max?"
        │                         │
        │                         └── use min/max to SKIP or OPEN each file
        ▼
file_N.csv              only the files that survived pruning get scanned
```

### Snapshots & time travel

Every write **appends** a new snapshot to the manifest list and **never
overwrites** old ones. Each snapshot records the full set of manifests visible
at that moment (all previous manifests + the new one). Because history is
append-only, querying `snapshot_id=2` reconstructs precisely what the table
contained after the second write — that's time travel, and it falls out
naturally from never mutating the past.

---

## The three files

- **`write.py`** — `write_batch(rows, filename)`: writes rows to a CSV,
  computes per-column min/max while writing, emits a manifest JSON, and
  appends a new snapshot to the manifest list.
- **`query.py`** — `query(column, value, snapshot_id=None)`: reads the
  manifest list, picks a snapshot, uses min/max to decide SKIP vs OPEN for
  each file (printing every decision), and scans only the survivors.
- **`demo.py`** — writes ~18 movies in four batches and runs several example
  queries plus a time-travel query, printing the full trace.

Everything is heavily commented — the code is meant to be *read*.

---

## Live demo (phone / browser)

`index.html` is a single self-contained file — all HTML/CSS/JS inline, no
external requests — that reimplements the exact same mechanism (min/max
pruning over append-only snapshots) in a few lines of vanilla JavaScript. The
Python remains the canonical reference; the page just mirrors it so you can
poke at it from a phone.

**Run it locally:** just open the file in a browser (double-click, or
`python -m http.server` and visit it). No build step, works offline.

**Host it free on GitHub Pages** (no server needed — the page is static):

1. Push this repo to GitHub (the `index.html` must be on your default branch,
   e.g. `main`).
2. Repo → **Settings → Pages**.
3. Under **Build and deployment**, set **Source = Deploy from a branch**,
   pick your default branch and the `/ (root)` folder, then **Save**.
4. Wait ~1 minute; your demo is live at
   `https://<username>.github.io/tip-of-the-iceberg/`.

That URL opens fine on a phone. (Vercel/Netlify also work — point them at the
repo with no build command and an empty output dir — but GitHub Pages needs
zero extra config here.)

---

## Try your own

```python
import write, query

write.write_batch([{"title": "Tenet", "year": 2020, "genre": "SciFi"}], "my_file.csv")
query.query("title", "Tenet")
query.query("year", 2020)
query.query("title", "Tenet", snapshot_id=1)  # time travel
```

Note the deliberate simplifications: one filter shape only (`column == value`),
one data file per write, CSV instead of Parquet, and the manifest list is a
single growing JSON rather than one file per snapshot. All chosen for
readability. The *mechanism* — min/max pruning over an append-only snapshot
log — is the real thing.
