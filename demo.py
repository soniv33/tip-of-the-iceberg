"""
demo.py — the full guided tour of the iceberg engine.
=====================================================

Run:  python demo.py

Walks through every capability, printing what happens at each step:

    1. CREATE a partitioned table (partition by genre).
    2. APPEND four batches -> four snapshots (real metadata tree on disk).
    3. Inspect the metadata tree (metadata.json -> manifest list -> manifests).
    4. QUERY with pruning: exact, prefix (LIKE), range, IN, partition, AND.
    5. TIME TRAVEL to an earlier snapshot.
    6. DELETE — merge-on-read (delete file) vs copy-on-write (rewrite).
    7. UPDATE and UPSERT (MERGE).
    8. COMPACT small files (and fold in delete files).
    9. SCHEMA EVOLUTION — add a column without rewriting old data.
   10. EXPIRE old snapshots and garbage-collect unreferenced files.

Everything is local files (CSV data + JSON metadata) so you can open the
warehouse/ directory afterwards and see exactly what the engine wrote.
"""

import os
import shutil

from iceberg import Catalog, Schema, Column, PartitionSpec, PartitionField
from iceberg import expressions as E

WAREHOUSE = "warehouse"
TABLE = "movies.films"

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


def rule(title):
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def on_disk(table):
    """Count what the engine has written under the table directory."""
    kinds = {"data (.csv)": 0, "manifests": 0, "manifest lists": 0,
             "delete files": 0, "metadata versions": 0}
    for root, _dirs, files in os.walk(table.location):
        for name in files:
            if name.startswith("delete-"):
                kinds["delete files"] += 1
            elif name.endswith(".csv"):
                kinds["data (.csv)"] += 1
            elif name.startswith("snap-"):
                kinds["manifest lists"] += 1
            elif name.startswith("manifest-"):
                kinds["manifests"] += 1
            elif name.endswith(".metadata.json"):
                kinds["metadata versions"] += 1
    return kinds


def main():
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)

    cat = Catalog(WAREHOUSE)

    # 1. CREATE ---------------------------------------------------------
    rule("1 · CREATE TABLE (partitioned by genre)")
    t = cat.create_table(
        TABLE,
        Schema([Column(1, "title", "string"), Column(2, "year", "int"),
                Column(3, "genre", "string")]),
        PartitionSpec([PartitionField("genre", "identity")]),
    )
    print(f"Created {TABLE} at {t.location}")

    # 2. APPEND ---------------------------------------------------------
    rule("2 · APPEND four batches -> four snapshots")
    for i, batch in enumerate(BATCHES, 1):
        snap = t.append(batch)
        print(f"  batch {i}: snapshot {snap['snapshot-id']} "
              f"(+{snap['summary']['added-data-files']} files, "
              f"{snap['summary']['total-records']} total records)")

    # 3. METADATA TREE --------------------------------------------------
    rule("3 · THE METADATA TREE (metadata.json -> manifest list -> manifests)")
    snap = t.current_snapshot()
    manifests = t._manifest_list(snap)
    print(f"  current snapshot {snap['snapshot-id']} -> manifest list "
          f"{os.path.basename(snap['manifest-list'])}")
    print(f"  manifest list references {len(manifests)} manifest(s):")
    for m in manifests:
        entries = len(__import__("json").load(open(t._abs(m)))["entries"])
        print(f"    {os.path.basename(m)}  ({entries} data file entr(y/ies))")
    print(f"  on disk: {on_disk(t)}")

    # 4. QUERIES WITH PRUNING ------------------------------------------
    rule("4 · QUERIES (watch the pruning decisions)")
    print("\n[exact]  title == 'Margin Call'")
    t.scan(E.Eq("title", "Margin Call")).explain()
    print("\n[prefix] title LIKE 'I%'  (a prefix is a range -> still prunes)")
    t.scan(E.StartsWith("title", "I")).explain()
    print("\n[range]  year >= 2014")
    t.scan(E.GtEq("year", 2014)).explain()
    print("\n[in]     genre IN ('Crime','Romance')")
    t.scan(E.In("genre", ["Crime", "Romance"])).explain()
    print("\n[partition] genre == 'SciFi'  (whole partitions skipped)")
    t.scan(E.Eq("genre", "SciFi")).explain()
    print("\n[and]    genre == 'Drama' AND year >= 2015")
    t.scan(E.And(E.Eq("genre", "Drama"), E.GtEq("year", 2015))).explain()

    # 5. TIME TRAVEL ----------------------------------------------------
    rule("5 · TIME TRAVEL (query snapshot #2, before 'Margin Call' existed)")
    second = t.snapshots()[1]["snapshot-id"]
    t.scan(E.Eq("title", "Margin Call"), snapshot_id=second).explain()

    # 6. MERGE-ON-READ DELETE ------------------------------------------
    rule("6 · DELETE (merge-on-read: writes a small delete file, data untouched)")
    live_data, live_del = t._live_entries(t.current_snapshot())
    t.delete(E.Eq("title", "Drive"), mode="merge-on-read")
    data2, del2 = t._live_entries(t.current_snapshot())
    print(f"  data files: {len(live_data)} -> {len(data2)} (unchanged), "
          f"delete files: {len(live_del)} -> {len(del2)} (one added)")
    print(f"  'Drive' visible now? {[r['title'] for r in t.scan(E.Eq('title','Drive')).rows()]} "
          f"(hidden at read time by the delete file)")

    # 7. COMPACTION -----------------------------------------------------
    rule("7 · COMPACT (merge many small files -> one per partition, fold deletes)")
    d0, x0 = (len(data2), len(del2))
    t.compact()
    data3, del3 = t._live_entries(t.current_snapshot())
    print(f"  LIVE data files: {d0} -> {len(data3)} (one per genre partition), "
          f"delete files: {x0} -> {len(del3)} (folded in)")
    print(f"  'Drive' still gone (now physically absent, not just hidden)? "
          f"{not t.scan(E.Eq('title','Drive')).rows()}")

    # 8. COPY-ON-WRITE DELETE / UPDATE / UPSERT ------------------------
    rule("8 · COPY-ON-WRITE DELETE, UPDATE, UPSERT")
    t.delete(E.Eq("title", "Zodiac"))          # rewrites data, no delete file
    print(f"  COW delete Zodiac -> visible? "
          f"{bool(t.scan(E.Eq('title','Zodiac')).rows())}")
    t.update(E.Eq("title", "Argo"), {"year": 2013})
    print(f"  UPDATE Argo.year=2013 -> {[r['year'] for r in t.scan(E.Eq('title','Argo')).rows()]}")
    t.upsert([{"title": "Her", "year": 2013, "genre": "Drama"},        # change genre
              {"title": "Tenet", "year": 2020, "genre": "SciFi"}],     # new row
             key_cols=["title"])
    print(f"  UPSERT Her(genre->Drama) + Tenet(new) -> Her genre now "
          f"{[r['genre'] for r in t.scan(E.Eq('title','Her')).rows()]}, "
          f"Tenet present? {bool(t.scan(E.Eq('title','Tenet')).rows())}")

    # 9. SCHEMA EVOLUTION ----------------------------------------------
    rule("9 · SCHEMA EVOLUTION (add a column, no data rewrite)")
    t.add_column("rating", "double")
    print(f"  added 'rating'; old rows read it as: "
          f"{[r.get('rating') for r in t.scan(E.Eq('title','Argo')).rows()]}")
    t.append([{"title": "Dune", "year": 2021, "genre": "SciFi", "rating": 8.0}])
    print(f"  new row carries it: "
          f"{[(r['title'], r['rating']) for r in t.scan(E.Eq('title','Dune')).rows()]}")

    # 10. EXPIRE --------------------------------------------------------
    rule("10 · EXPIRE SNAPSHOTS (drop history, garbage-collect files)")
    print(f"  snapshots before: {len(t.snapshots())}, files: {on_disk(t)}")
    removed = t.expire_snapshots(keep_last=1)
    print(f"  expired to 1 snapshot; garbage-collected {removed} files")
    print(f"  snapshots after:  {len(t.snapshots())}, files: {on_disk(t)}")
    print(f"  table still fully queryable: "
          f"{len(t.scan().rows())} rows, e.g. Tenet? "
          f"{bool(t.scan(E.Eq('title','Tenet')).rows())}")

    print("\nDone. Explore warehouse/movies/films/ to see the real files.")


if __name__ == "__main__":
    main()
