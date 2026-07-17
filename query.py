"""
query.py — the "read path" of our tiny Iceberg clone.
============================================================

This is where the metadata we so carefully wrote in write.py pays off.

A naive query engine, asked for `title == 'Margin Call'`, would open every
data file and scan every row. Iceberg's insight: the manifests already know
the min/max of each column in each file, so we can *prove* that many files
cannot possibly contain the answer and skip them without ever opening them.
That's "predicate pushdown" / "min-max pruning".

The read path here is:

    1. Read the manifest list and pick a snapshot (default: the latest).
       Picking an *older* snapshot = time travel.
    2. Collect every data file visible in that snapshot (via its manifests).
    3. For each data file, use its column min/max to decide SKIP vs OPEN,
       and print that decision so the mechanism is visible.
    4. Only actually scan rows in the files that survive pruning.

We only implement one filter shape — `column == value` — because it's enough
to show the whole idea and keeps the code readable.
"""

import csv
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MANIFEST_LIST_PATH = os.path.join(BASE_DIR, "manifests", "manifest_list.json")


def _load_manifest_list():
    """Read the table history written by write.py."""
    if not os.path.exists(MANIFEST_LIST_PATH):
        raise FileNotFoundError(
            "No manifest list found. Run write.py / demo.py first."
        )
    with open(MANIFEST_LIST_PATH) as f:
        return json.load(f)


def _get_snapshot(manifest_list, snapshot_id=None):
    """
    Return the requested snapshot, or the most recent one if snapshot_id is
    None. Passing an older snapshot_id is how you 'time travel' to see the
    table as it existed after an earlier write.
    """
    snapshots = manifest_list["snapshots"]
    if not snapshots:
        raise ValueError("Manifest list has no snapshots yet.")

    if snapshot_id is None:
        return snapshots[-1]  # latest

    for snap in snapshots:
        if snap["snapshot_id"] == snapshot_id:
            return snap
    raise ValueError(f"No snapshot with id {snapshot_id}.")


def _collect_data_files(snapshot):
    """
    Walk every manifest listed in the snapshot and gather the data-file
    descriptors (path + record_count + column_stats) they contain.
    """
    data_files = []
    for manifest_rel_path in snapshot["manifests"]:
        manifest_path = os.path.join(BASE_DIR, manifest_rel_path)
        with open(manifest_path) as f:
            manifest = json.load(f)
        data_files.extend(manifest["data_files"])
    return data_files


def _can_skip(data_file, column, value):
    """
    The pruning decision, in one place.

    Given a file's stats for `column` = {min, max}, an equality predicate
    `column == value` can only be satisfied if `min <= value <= max`.
    If value falls outside that inclusive range, the file provably contains
    no matching row and can be skipped.

    Returns (should_skip, min, max). We return the range too so the caller
    can print a nice explanation.
    """
    stats = data_file["column_stats"].get(column)
    if stats is None:
        # We have no statistics for this column, so we can't rule the file
        # out. The safe answer is always "don't skip" — never skip a file
        # you can't prove is irrelevant.
        return False, None, None

    lo, hi = stats["min"], stats["max"]
    should_skip = not (lo <= value <= hi)
    return should_skip, lo, hi


def _scan_file(data_file, column, value):
    """
    Actually open a surviving data file and return the rows that match.
    This is the expensive part we've been working to avoid doing unnecessarily.
    """
    matches = []
    data_path = os.path.join(BASE_DIR, data_file["path"])
    with open(data_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # CSV reads everything back as strings, so compare as strings.
            # (We stringify `value` too, so `year == 2012` still works.)
            if row[column] == str(value):
                matches.append(row)
    return matches


def query(column, value, snapshot_id=None):
    """
    Run `column == value` against the table and print the full pruning trace.

    Args:
        column:      the column to filter on, e.g. "title"
        value:       the value to match, e.g. "Margin Call"
        snapshot_id: optional — query an older snapshot to time travel.
                     None means "use the latest snapshot".

    Returns:
        A list of matching rows (dicts).
    """
    manifest_list = _load_manifest_list()
    snapshot = _get_snapshot(manifest_list, snapshot_id)
    data_files = _collect_data_files(snapshot)

    print(
        f"\nQUERY  {column} == {value!r}   "
        f"(snapshot #{snapshot['snapshot_id']}, {len(data_files)} data file(s))"
    )
    print("-" * 68)

    results = []
    skipped = 0
    for data_file in data_files:
        name = os.path.basename(data_file["path"])
        should_skip, lo, hi = _can_skip(data_file, column, value)

        if should_skip:
            skipped += 1
            print(
                f"  SKIP  {name:<14} "
                f"(range {lo}–{hi}, doesn't contain {value!r})"
            )
            continue

        # Survived pruning — we have to actually look inside.
        print(
            f"  OPEN  {name:<14} "
            f"(range {lo}–{hi}, checking rows)"
        )
        matches = _scan_file(data_file, column, value)
        for m in matches:
            print(f"          -> match: {m}")
        results.extend(matches)

    print("-" * 68)
    print(
        f"  Pruned {skipped}/{len(data_files)} files without opening them. "
        f"Found {len(results)} matching row(s)."
    )
    return results


if __name__ == "__main__":
    # Run a sample query if invoked directly (assumes data already written).
    query("title", "Margin Call")
