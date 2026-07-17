"""
write.py — the "write path" of our tiny Iceberg clone.
============================================================

In real Apache Iceberg, when you write data you don't just drop a Parquet
file somewhere. Iceberg *also* records metadata about that file so that later
readers can be smart about which files they even need to open. That metadata
is the whole magic trick. This module reproduces the essential idea:

    1. Write the rows to a data file (a CSV here, for readability).
    2. While writing, compute per-column min/max statistics.
    3. Write a "manifest" — a small JSON that describes that one data file:
       its path, how many rows it has, and the min/max of every column.
    4. Append a new "snapshot" to the "manifest list" — a running log of
       every write we've ever done. We never overwrite old snapshots, which
       is exactly what makes time travel possible later.

The mapping to real Iceberg:

    manifest list  (JSON here)  ->  Iceberg manifest list (Avro)
    manifest       (JSON here)  ->  Iceberg manifest       (Avro)
    data file      (CSV here)   ->  Iceberg data file      (Parquet)

Everything is local files and the Python standard library. Nothing clever —
the point is that you can read every line and see the mechanism.
"""

import csv
import json
import os
import time

# ---------------------------------------------------------------------------
# Where things live on disk. We keep data and metadata in separate folders,
# just like Iceberg keeps a data/ dir and a metadata/ dir.
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
MANIFEST_DIR = os.path.join(BASE_DIR, "manifests")

# The manifest list is a single file that grows over time. Each write appends
# one new snapshot to it. This is our table's "history".
MANIFEST_LIST_PATH = os.path.join(MANIFEST_DIR, "manifest_list.json")


def _ensure_dirs():
    """Make sure data/ and manifests/ exist. Safe to call repeatedly."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MANIFEST_DIR, exist_ok=True)


def _compute_column_stats(rows):
    """
    Compute the min and max of every column across all rows in this batch.

    This is the heart of the whole pruning idea. If a data file only contains
    movies whose titles run from "Argo" to "Drive", a reader looking for
    "Margin Call" can prove — without opening the file — that it isn't there,
    because "Margin Call" > "Drive" alphabetically.

    Python's min()/max() work for both strings (lexicographic order) and
    numbers (numeric order), which is all we need for columns like `title`
    (str) and `year` (int).

    Returns a dict shaped like:
        {
          "title": {"min": "Argo",  "max": "Drive"},
          "year":  {"min": 2011,    "max": 2012},
        }
    """
    stats = {}
    if not rows:
        return stats

    # Assume every row has the same set of columns (true for our demo data).
    columns = rows[0].keys()

    for column in columns:
        values = [row[column] for row in rows]
        stats[column] = {
            "min": min(values),
            "max": max(values),
        }
    return stats


def _write_data_file(rows, filename):
    """
    Write the batch of rows to data/<filename> as CSV and return the path.

    In real Iceberg this would be a Parquet file; CSV keeps the demo
    dependency-free and lets you cat the file to see exactly what's in it.
    """
    data_path = os.path.join(DATA_DIR, filename)
    columns = list(rows[0].keys())

    with open(data_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    return data_path


def _load_manifest_list():
    """
    Read the current manifest list (the table history), or return a fresh,
    empty one if this is the very first write.

    Structure:
        {
          "format": "tip-of-the-iceberg/v1",
          "snapshots": [ <snapshot>, <snapshot>, ... ]
        }

    Each snapshot looks like:
        {
          "snapshot_id": 1,
          "timestamp": 1700000000.0,
          "parent_snapshot_id": null,
          "manifests": ["manifests/manifest_1.json", ...]  # ALL manifests
        }                                                   # visible at this
                                                            # point in time
    """
    if not os.path.exists(MANIFEST_LIST_PATH):
        return {"format": "tip-of-the-iceberg/v1", "snapshots": []}
    with open(MANIFEST_LIST_PATH) as f:
        return json.load(f)


def _save_manifest_list(manifest_list):
    """Persist the manifest list back to disk (pretty-printed for reading)."""
    with open(MANIFEST_LIST_PATH, "w") as f:
        json.dump(manifest_list, f, indent=2)


def write_batch(rows, filename):
    """
    The public entry point. Write one batch of rows and record all the
    metadata that makes pruning and time travel possible.

    Args:
        rows:     a list of dicts, e.g.
                  [{"title": "Argo", "year": 2012, "genre": "Thriller"}, ...]
        filename: the data file name to create under data/, e.g. "file_1.csv"

    Returns:
        The snapshot dict that was just appended to the manifest list.

    Steps (each one maps to a real Iceberg concept):
    """
    if not rows:
        raise ValueError("write_batch called with no rows to write.")

    _ensure_dirs()

    # --- Step 1: write the actual data file -----------------------------
    data_path = _write_data_file(rows, filename)

    # --- Step 2: compute per-column min/max while we have the rows ------
    stats = _compute_column_stats(rows)

    # --- Step 3: write a manifest describing THIS data file -------------
    # We name manifests by their position in history so old ones are never
    # overwritten. manifest_1.json, manifest_2.json, and so on.
    manifest_list = _load_manifest_list()
    next_index = len(manifest_list["snapshots"]) + 1

    manifest_name = f"manifest_{next_index}.json"
    manifest_path = os.path.join(MANIFEST_DIR, manifest_name)

    manifest = {
        # A manifest can describe many data files; ours describes one per
        # write, but we keep it as a list so the structure matches Iceberg.
        "data_files": [
            {
                # Store a repo-relative path so the files are portable.
                "path": os.path.relpath(data_path, BASE_DIR),
                "record_count": len(rows),
                "column_stats": stats,
            }
        ]
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # --- Step 4: append a NEW snapshot to the manifest list -------------
    # The key move: a snapshot lists EVERY manifest visible at that moment,
    # i.e. all previous manifests plus the one we just wrote. Because we only
    # ever append, snapshot #2 still remembers exactly what the table looked
    # like at snapshot #1 — that's time travel, for free.
    previous_snapshots = manifest_list["snapshots"]
    previous_manifests = (
        previous_snapshots[-1]["manifests"] if previous_snapshots else []
    )
    parent_id = (
        previous_snapshots[-1]["snapshot_id"] if previous_snapshots else None
    )

    snapshot = {
        "snapshot_id": next_index,
        "timestamp": time.time(),
        "parent_snapshot_id": parent_id,
        "manifests": previous_manifests + [os.path.join("manifests", manifest_name)],
    }
    manifest_list["snapshots"].append(snapshot)
    _save_manifest_list(manifest_list)

    # A little human-readable trace so demo output shows what happened.
    col_summary = ", ".join(
        f"{col} [{s['min']}..{s['max']}]" for col, s in stats.items()
    )
    print(
        f"WROTE snapshot #{snapshot['snapshot_id']}: "
        f"{len(rows)} rows -> data/{filename} | {col_summary}"
    )

    return snapshot


if __name__ == "__main__":
    # Tiny smoke test if you run `python write.py` directly.
    write_batch(
        [
            {"title": "Argo", "year": 2012, "genre": "Thriller"},
            {"title": "Drive", "year": 2011, "genre": "Crime"},
        ],
        "file_smoke.csv",
    )
