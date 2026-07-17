"""
iceberg.table — the engine: catalog, metadata tree, scans, and operations.
==========================================================================

This is where the Iceberg mechanism actually lives. The on-disk layout under a
table directory mirrors real Iceberg:

    <table>/
      data/<partition>/<uuid>.csv          data files (CSV here vs Parquet)
      data/<partition>/delete-<uuid>.csv   merge-on-read equality delete files
      metadata/vN.metadata.json            versioned table metadata (the "root")
      metadata/snap-<id>.json              a snapshot's manifest LIST
      metadata/manifest-<uuid>.json        a manifest (list of file entries)
      version-hint.text                    which metadata version is current

The three-level metadata tree — metadata.json -> manifest list -> manifests ->
data files — is exactly Iceberg's. Each write creates a new immutable snapshot;
nothing is ever mutated in place, which is what gives us time travel.

Reads:  Table.scan(row_filter, snapshot_id/as_of).plan()/.rows()
Writes: append, overwrite, delete (copy-on-write OR merge-on-read),
        update, upsert
Maint.: compact, expire_snapshots, add_column (schema evolution)
"""

import json
import os
import time
import uuid

from .schema import Schema, PartitionSpec, compute_stats
from .expressions import AlwaysTrue, FileMetrics


# ===========================================================================
# Small JSON/CSV helpers.
# ===========================================================================
def _read_json(path):
    with open(path) as f:
        return json.load(f)


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def _now_ms():
    return int(time.time() * 1000)


def _uuid():
    return uuid.uuid4().hex[:12]


# ===========================================================================
# Catalog — tracks tables and each table's current metadata pointer.
# ===========================================================================
class Catalog:
    """A local catalog backed by a warehouse directory + a catalog.json index."""

    def __init__(self, warehouse):
        self.warehouse = os.path.abspath(warehouse)
        os.makedirs(self.warehouse, exist_ok=True)
        self.index_path = os.path.join(self.warehouse, "catalog.json")
        self.index = _read_json(self.index_path) if os.path.exists(self.index_path) else {"tables": {}}

    def _save_index(self):
        _write_json(self.index_path, self.index)

    def _location(self, identifier):
        return os.path.join(self.warehouse, *identifier.split("."))

    def create_table(self, identifier, schema, partition_spec=None, properties=None):
        if identifier in self.index["tables"]:
            raise ValueError(f"table {identifier} already exists")
        spec = partition_spec or PartitionSpec()
        location = self._location(identifier)
        os.makedirs(os.path.join(location, "data"), exist_ok=True)
        os.makedirs(os.path.join(location, "metadata"), exist_ok=True)

        metadata = {
            "format-version": 2,
            "table-uuid": uuid.uuid4().hex,
            "location": location,
            "last-updated-ms": _now_ms(),
            "last-column-id": schema.last_column_id(),
            "schema": schema.to_dict(),
            "partition-spec": spec.to_dict(),
            "properties": properties or {},
            "current-snapshot-id": None,
            "last-sequence-number": 0,
            "snapshots": [],
            "snapshot-log": [],
        }
        table = Table(self, identifier, location, metadata)
        table._write_metadata(metadata, version=0)
        self.index["tables"][identifier] = {"location": location}
        self._save_index()
        return table

    def load_table(self, identifier):
        location = self.index["tables"][identifier]["location"]
        hint = os.path.join(location, "version-hint.text")
        with open(hint) as f:
            version = int(f.read().strip())
        metadata = _read_json(os.path.join(location, "metadata", f"v{version}.metadata.json"))
        return Table(self, identifier, location, metadata)

    def table_exists(self, identifier):
        return identifier in self.index["tables"]

    def list_tables(self):
        return sorted(self.index["tables"])


# ===========================================================================
# Scan — plans which files to read (pruning) and returns matching rows.
# ===========================================================================
class Scan:
    """A planned read against one snapshot with one row filter."""

    def __init__(self, table, snapshot, row_filter):
        self.table = table
        self.snapshot = snapshot
        self.filter = row_filter

    def plan(self):
        """
        Decide SKIP vs OPEN for each live data file, using its stats.
        Returns a list of decisions (dicts) — the pruning trace.
        """
        decisions = []
        if self.snapshot is None:
            return decisions
        data_files, _deletes = self.table._live_entries(self.snapshot)
        part_cols = self.table.spec.columns()
        for entry in data_files:
            metrics = FileMetrics(entry)
            keep = self.filter.can_match(metrics)
            # Was pruning driven by a partition column? (nice for the trace)
            by_partition = bool(self.filter.refs() & part_cols)
            decisions.append({
                "path": entry["file_path"],
                "open": keep,
                "partition": entry.get("partition", {}),
                "bounds": self.table._bounds_summary(entry, self.filter.refs()),
                "by_partition": by_partition,
            })
        return decisions

    def rows(self):
        """Actually read the surviving files, apply deletes, and filter rows."""
        if self.snapshot is None:
            return []
        data_files, deletes = self.table._live_entries(self.snapshot)
        results = []
        for entry in data_files:
            if not self.filter.can_match(FileMetrics(entry)):
                continue  # pruned — never opened
            for row in self.table._read_live_rows(entry, deletes):
                if self.filter.evaluate(row):
                    results.append(row)
        return results

    def explain(self):
        """Print the pruning trace, then return the rows (for demos/CLI)."""
        decisions = self.plan()
        opened = sum(1 for d in decisions if d["open"])
        print(f"  scan {self.filter}  (snapshot {self.snapshot['snapshot-id'] if self.snapshot else None}, "
              f"{len(decisions)} data file(s))")
        for d in decisions:
            name = os.path.basename(d["path"])
            tag = "OPEN " if d["open"] else "SKIP "
            why = d["bounds"] or "no stats"
            kind = "partition " if d["by_partition"] else ""
            verb = "checking rows" if d["open"] else f"{kind}stats rule it out"
            print(f"    {tag} {name:<26} ({why} — {verb})")
        rows = self.rows()
        print(f"    -> opened {opened}/{len(decisions)}, {len(rows)} matching row(s)")
        return rows


# ===========================================================================
# Table — the thing you read and write.
# ===========================================================================
class Table:
    def __init__(self, catalog, identifier, location, metadata):
        self.catalog = catalog
        self.identifier = identifier
        self.location = location
        self.metadata = metadata
        self.schema = Schema.from_dict(metadata["schema"])
        self.spec = PartitionSpec.from_dict(metadata["partition-spec"])

    # --- paths --------------------------------------------------------
    def _meta_dir(self):
        return os.path.join(self.location, "metadata")

    def _data_dir(self):
        return os.path.join(self.location, "data")

    def _abs(self, rel):
        return os.path.join(self.location, rel)

    def _rel(self, abspath):
        return os.path.relpath(abspath, self.location)

    # --- metadata versioning -----------------------------------------
    def _current_version(self):
        hint = os.path.join(self.location, "version-hint.text")
        if not os.path.exists(hint):
            return -1
        with open(hint) as f:
            return int(f.read().strip())

    def _write_metadata(self, metadata, version=None):
        if version is None:
            version = self._current_version() + 1
        metadata["last-updated-ms"] = _now_ms()
        _write_json(os.path.join(self._meta_dir(), f"v{version}.metadata.json"), metadata)
        with open(os.path.join(self.location, "version-hint.text"), "w") as f:
            f.write(str(version))
        self.metadata = metadata
        self.schema = Schema.from_dict(metadata["schema"])
        self.spec = PartitionSpec.from_dict(metadata["partition-spec"])

    def refresh(self):
        self.metadata = self.catalog.load_table(self.identifier).metadata
        self.schema = Schema.from_dict(self.metadata["schema"])
        self.spec = PartitionSpec.from_dict(self.metadata["partition-spec"])
        return self

    # --- snapshots ----------------------------------------------------
    def current_snapshot(self):
        sid = self.metadata["current-snapshot-id"]
        return self._snapshot_by_id(sid) if sid is not None else None

    def snapshots(self):
        return self.metadata["snapshots"]

    def _snapshot_by_id(self, sid):
        for s in self.metadata["snapshots"]:
            if s["snapshot-id"] == sid:
                return s
        raise ValueError(f"no snapshot {sid}")

    def _snapshot_as_of(self, timestamp_ms):
        """The snapshot that was current at a given time — time travel by clock."""
        chosen = None
        for entry in self.metadata["snapshot-log"]:
            if entry["timestamp-ms"] <= timestamp_ms:
                chosen = entry["snapshot-id"]
        if chosen is None:
            raise ValueError("no snapshot at or before that time")
        return self._snapshot_by_id(chosen)

    # --- manifests / entries -----------------------------------------
    def _manifest_list(self, snapshot):
        return _read_json(self._abs(snapshot["manifest-list"]))["manifests"]

    def _live_entries(self, snapshot):
        """Read every manifest of a snapshot; split into data + delete entries."""
        data, deletes = [], []
        for man_rel in self._manifest_list(snapshot):
            manifest = _read_json(self._abs(man_rel))
            for entry in manifest["entries"]:
                (deletes if entry["content"] != "data" else data).append(entry)
        return data, deletes

    def _read_file_rows(self, entry):
        """Read one CSV data file back into typed dict rows."""
        import csv
        rows = []
        with open(self._abs(entry["file_path"]), newline="") as f:
            for raw in csv.DictReader(f):
                rows.append(self.schema.cast_row(raw))
        return rows

    def _read_live_rows(self, entry, delete_entries):
        """
        Rows of a data file after applying merge-on-read equality deletes.

        An equality delete applies to a data file only if its sequence number
        is GREATER than the data file's — i.e. the delete came later. That's
        why re-inserting a deleted key (an append with a higher seq) brings it
        back: older deletes don't touch newer data.
        """
        rows = self._read_file_rows(entry)
        data_seq = entry["sequence_number"]
        applicable = [d for d in delete_entries if d["sequence_number"] > data_seq]
        if not applicable:
            return rows
        # Build the set of deleted key-tuples per equality-column set.
        deleted = []  # list of (cols, set_of_value_tuples)
        for d in applicable:
            cols = d["equality_ids"]
            keys = set()
            for raw in self._read_delete_rows(d, cols):
                keys.add(tuple(raw[c] for c in cols))
            deleted.append((cols, keys))
        out = []
        for row in rows:
            if any(tuple(row[c] for c in cols) in keys for cols, keys in deleted):
                continue
            out.append(row)
        return out

    def _read_delete_rows(self, entry, cols):
        import csv
        rows = []
        with open(self._abs(entry["file_path"]), newline="") as f:
            for raw in csv.DictReader(f):
                rows.append(self.schema.cast_row(raw))
        return rows

    def _bounds_summary(self, entry, cols):
        """Human-readable min..max for the columns a filter references."""
        parts = []
        for c in sorted(cols):
            lo = entry.get("lower_bounds", {}).get(c)
            hi = entry.get("upper_bounds", {}).get(c)
            if lo is not None or hi is not None:
                parts.append(f"{c} [{lo}..{hi}]")
        return ", ".join(parts)

    # --- writing files -----------------------------------------------
    def _write_data_file(self, rows, partition, seq):
        import csv
        part_path = self.spec.path_for(partition)
        directory = os.path.join(self._data_dir(), part_path) if part_path else self._data_dir()
        os.makedirs(directory, exist_ok=True)
        abspath = os.path.join(directory, f"{_uuid()}.csv")
        with open(abspath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.schema.names())
            writer.writeheader()
            for row in rows:
                writer.writerow(self.schema.format_row(row))
        stats = compute_stats(rows, self.schema)
        return {
            "content": "data",
            "file_path": self._rel(abspath),
            "partition": partition,
            "sequence_number": seq,
            **stats,
        }

    def _write_delete_file(self, key_rows, cols, seq):
        import csv
        abspath = os.path.join(self._data_dir(), f"delete-{_uuid()}.csv")
        with open(abspath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.schema.names())
            writer.writeheader()
            for row in key_rows:
                writer.writerow(self.schema.format_row(row))
        return {
            "content": "equality-deletes",
            "file_path": self._rel(abspath),
            "partition": {},                 # global equality delete
            "sequence_number": seq,
            "equality_ids": cols,
            "record_count": len(key_rows),
        }

    def _write_manifest(self, content, entries):
        rel = os.path.join("metadata", f"manifest-{_uuid()}.json")
        _write_json(self._abs(rel), {"content": content, "entries": entries})
        return rel

    def _commit(self, operation, manifest_rels, summary_extra=None):
        """Create a new snapshot referencing the given manifests and persist it."""
        seq = self.metadata["last-sequence-number"] + 1
        snapshot_id = int(time.time() * 1000) * 1000 + len(self.metadata["snapshots"])
        parent = self.metadata["current-snapshot-id"]
        list_rel = os.path.join("metadata", f"snap-{snapshot_id}.json")
        _write_json(self._abs(list_rel), {"manifests": manifest_rels})

        # Recompute totals for the summary from the new live set.
        data, deletes = [], []
        for man_rel in manifest_rels:
            m = _read_json(self._abs(man_rel))
            for e in m["entries"]:
                (deletes if e["content"] != "data" else data).append(e)
        summary = {
            "operation": operation,
            "total-data-files": len(data),
            "total-delete-files": len(deletes),
            "total-records": sum(e.get("record_count", 0) for e in data),
        }
        summary.update(summary_extra or {})

        snapshot = {
            "snapshot-id": snapshot_id,
            "parent-snapshot-id": parent,
            "sequence-number": seq,
            "timestamp-ms": _now_ms(),
            "manifest-list": list_rel,
            "summary": summary,
        }
        meta = json.loads(json.dumps(self.metadata))  # deep copy
        meta["snapshots"].append(snapshot)
        meta["snapshot-log"].append({"timestamp-ms": snapshot["timestamp-ms"], "snapshot-id": snapshot_id})
        meta["current-snapshot-id"] = snapshot_id
        meta["last-sequence-number"] = seq
        self._write_metadata(meta)
        return snapshot

    def _group_by_partition(self, rows):
        groups = {}
        for row in rows:
            part = self.spec.partition_for(row)
            groups.setdefault(tuple(sorted(part.items())), (part, []))[1].append(row)
        return [(part, rs) for part, rs in groups.values()]

    # --- public reads -------------------------------------------------
    def scan(self, row_filter=None, snapshot_id=None, as_of=None):
        if snapshot_id is not None:
            snapshot = self._snapshot_by_id(snapshot_id)
        elif as_of is not None:
            snapshot = self._snapshot_as_of(as_of)
        else:
            snapshot = self.current_snapshot()
        return Scan(self, snapshot, row_filter or AlwaysTrue())

    def all_rows(self, snapshot=None):
        """Every live row of a snapshot (deletes applied). Used by rewrites."""
        snapshot = snapshot or self.current_snapshot()
        if snapshot is None:
            return []
        data, deletes = self._live_entries(snapshot)
        rows = []
        for entry in data:
            rows.extend(self._read_live_rows(entry, deletes))
        return rows

    # --- public writes ------------------------------------------------
    def append(self, rows):
        """
        Add rows as a NEW snapshot. Incremental: we write a new data manifest
        for just these rows and reuse the parent snapshot's manifests, so old
        metadata is never rewritten (this is how real Iceberg appends stay cheap).
        """
        seq = self.metadata["last-sequence-number"] + 1
        entries = [self._write_data_file(rs, part, seq) for part, rs in self._group_by_partition(rows)]
        new_manifest = self._write_manifest("data", entries)

        parent = self.current_snapshot()
        reused = self._manifest_list(parent) if parent else []
        return self._commit("append", reused + [new_manifest],
                            {"added-data-files": len(entries), "added-records": len(rows)})

    def _rewrite(self, rows, operation):
        """
        Regenerate the whole live data set as fresh files (grouped by
        partition), dropping any delete files. This is the copy-on-write /
        replace path shared by delete(COW), update, upsert and compact.

        Real Iceberg rewrites only the affected files; we rewrite everything
        because the datasets here are tiny and it keeps the code obvious.
        """
        seq = self.metadata["last-sequence-number"] + 1
        entries = [self._write_data_file(rs, part, seq) for part, rs in self._group_by_partition(rows)]
        manifest = self._write_manifest("data", entries)
        return self._commit(operation, [manifest],
                            {"total-records": len(rows)})

    def overwrite(self, rows, overwrite_filter):
        """Replace every row matching the filter with `rows` (copy-on-write)."""
        kept = [r for r in self.all_rows() if not overwrite_filter.evaluate(r)]
        return self._rewrite(kept + list(rows), "overwrite")

    def delete(self, row_filter, mode="copy-on-write"):
        """
        Delete rows matching a filter.

        copy-on-write: physically rewrite the data without those rows.
        merge-on-read: write a small equality delete file; data files are left
                       untouched and the delete is applied at read time. Only
                       equality filters (Eq / And-of-Eq) are supported for MOR,
                       which is exactly what equality delete files encode.
        """
        if mode == "merge-on-read":
            cols, key = self._equality_key(row_filter)
            seq = self.metadata["last-sequence-number"] + 1
            delete_entry = self._write_delete_file([key], cols, seq)
            new_manifest = self._write_manifest("equality-deletes", [delete_entry])
            parent = self.current_snapshot()
            reused = self._manifest_list(parent) if parent else []
            return self._commit("delete", reused + [new_manifest],
                                {"added-delete-files": 1})
        # copy-on-write
        kept = [r for r in self.all_rows() if not row_filter.evaluate(r)]
        return self._rewrite(kept, "delete")

    def _equality_key(self, row_filter):
        """Extract {col: value} from an Eq or And-of-Eq filter for MOR deletes."""
        from .expressions import Eq, And
        preds = row_filter.children if isinstance(row_filter, And) else [row_filter]
        key = {}
        for p in preds:
            if not isinstance(p, Eq):
                raise ValueError("merge-on-read delete needs an equality filter")
            key[p.name] = p.value
        # Fill unspecified columns with None so the CSV has every column.
        full = {c: key.get(c) for c in self.schema.names()}
        return list(key.keys()), full

    def update(self, row_filter, assignments):
        """SET columns for matching rows (copy-on-write delete+insert)."""
        rows = self.all_rows()
        for r in rows:
            if row_filter.evaluate(r):
                r.update(assignments)
        return self._rewrite(rows, "overwrite")

    def upsert(self, rows, key_cols):
        """
        Merge rows by key: matching keys are replaced, new keys inserted
        (copy-on-write). This is `MERGE INTO` in miniature.
        """
        incoming = {tuple(r[c] for c in key_cols): r for r in rows}
        merged = []
        for r in self.all_rows():
            k = tuple(r[c] for c in key_cols)
            if k in incoming:
                continue  # will be replaced by the incoming version
            merged.append(r)
        merged.extend(incoming.values())
        return self._rewrite(merged, "overwrite")

    # --- maintenance --------------------------------------------------
    def compact(self):
        """
        Rewrite each partition's many small files (and fold in any merge-on-read
        delete files) into one file per partition, as a new snapshot. Older
        snapshots still reference the old files, so time travel keeps working
        until you expire snapshots.
        """
        return self._rewrite(self.all_rows(), "replace")

    def expire_snapshots(self, keep_last=1):
        """
        Drop all but the most recent `keep_last` snapshots, then garbage-collect
        data/manifest/delete files no longer referenced by any surviving
        snapshot. This is what actually reclaims disk from old versions.
        """
        snapshots = self.metadata["snapshots"]
        keep = snapshots[-keep_last:] if keep_last else snapshots
        keep_ids = {s["snapshot-id"] for s in keep}

        # Everything the survivors still reference.
        referenced = set()
        for snap in keep:
            list_rel = snap["manifest-list"]
            referenced.add(list_rel)
            for man_rel in _read_json(self._abs(list_rel))["manifests"]:
                referenced.add(man_rel)
                for entry in _read_json(self._abs(man_rel))["entries"]:
                    referenced.add(entry["file_path"])

        removed = 0
        for root, _dirs, files in os.walk(self.location):
            for name in files:
                rel = self._rel(os.path.join(root, name))
                if rel.startswith("metadata" + os.sep):
                    # keep versioned metadata + version-hint; drop orphan snap/manifest
                    base = os.path.basename(rel)
                    if base.startswith(("snap-", "manifest-")) and rel not in referenced:
                        os.remove(self._abs(rel))
                        removed += 1
                elif rel.startswith("data" + os.sep):
                    if rel not in referenced:
                        os.remove(self._abs(rel))
                        removed += 1

        meta = json.loads(json.dumps(self.metadata))
        meta["snapshots"] = keep
        meta["snapshot-log"] = [e for e in meta["snapshot-log"] if e["snapshot-id"] in keep_ids]
        if meta["current-snapshot-id"] not in keep_ids:
            meta["current-snapshot-id"] = keep[-1]["snapshot-id"] if keep else None
        self._write_metadata(meta)
        return removed

    def add_column(self, name, type_, required=False):
        """
        Schema evolution: add a column. Existing data files don't have it, and
        reading them returns None for the new column (safe because Iceberg — and
        our reader — fill missing columns). No data is rewritten.
        """
        from .schema import Column
        meta = json.loads(json.dumps(self.metadata))
        new_id = meta["last-column-id"] + 1
        meta["schema"]["columns"].append(Column(new_id, name, type_, required).to_dict())
        meta["last-column-id"] = new_id
        self._write_metadata(meta)
        return self
