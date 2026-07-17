"""
demo.py — end-to-end walkthrough of the whole mechanism.
============================================================

Run me with:  python demo.py

What this does:
    1. Starts from a clean slate (deletes any data/ and manifests/ from a
       previous run so the demo is reproducible).
    2. Writes ~18 movies in FOUR separate batches — simulating four writes
       to the table over time. Each batch becomes its own data file, its own
       manifest, and its own snapshot in the manifest list.
    3. Runs a few example queries and prints the full pruning trace, so you
       can watch which files get SKIPped vs OPENed and why.
    4. Demonstrates time travel by querying an older snapshot.

The batches are deliberately grouped so their title ranges don't fully
overlap — that's what makes min/max pruning able to skip whole files.
"""

import os
import shutil

import write
import query


# ---------------------------------------------------------------------------
# Sample data: ~18 movies, grouped into four batches. Grouping alphabetically
# by title keeps each file's [min title .. max title] range fairly tight, so
# pruning has something to bite on. In the real world your data wouldn't be
# this tidy, but the mechanism is identical — it just prunes less.
# ---------------------------------------------------------------------------
BATCH_1 = [  # titles roughly A–D
    {"title": "Amelie", "year": 2001, "genre": "Romance"},
    {"title": "Argo", "year": 2012, "genre": "Thriller"},
    {"title": "Boyhood", "year": 2014, "genre": "Drama"},
    {"title": "Drive", "year": 2011, "genre": "Crime"},
]
BATCH_2 = [  # titles roughly F–H
    {"title": "Fargo", "year": 1996, "genre": "Crime"},
    {"title": "Gattaca", "year": 1997, "genre": "SciFi"},
    {"title": "Gravity", "year": 2013, "genre": "SciFi"},
    {"title": "Her", "year": 2013, "genre": "Romance"},
]
BATCH_3 = [  # titles roughly I–M
    {"title": "Inception", "year": 2010, "genre": "SciFi"},
    {"title": "Interstellar", "year": 2014, "genre": "SciFi"},
    {"title": "Magnolia", "year": 1999, "genre": "Drama"},
    {"title": "Margin Call", "year": 2011, "genre": "Drama"},
    {"title": "Moonlight", "year": 2016, "genre": "Drama"},
]
BATCH_4 = [  # titles roughly N–Z
    {"title": "Nightcrawler", "year": 2014, "genre": "Thriller"},
    {"title": "Prisoners", "year": 2013, "genre": "Thriller"},
    {"title": "Sicario", "year": 2015, "genre": "Thriller"},
    {"title": "Whiplash", "year": 2014, "genre": "Drama"},
    {"title": "Zodiac", "year": 2007, "genre": "Thriller"},
]


def _reset():
    """Delete previous demo output so each run starts fresh and reproducible."""
    for d in (write.DATA_DIR, write.MANIFEST_DIR):
        if os.path.exists(d):
            shutil.rmtree(d)


def main():
    print("=" * 68)
    print("  tip-of-the-iceberg :: end-to-end demo")
    print("=" * 68)

    _reset()

    # --- Write phase: four writes over "time" --------------------------
    print("\n[1] WRITE PHASE — four batches, four snapshots\n")
    write.write_batch(BATCH_1, "file_1.csv")  # snapshot #1
    write.write_batch(BATCH_2, "file_2.csv")  # snapshot #2
    write.write_batch(BATCH_3, "file_3.csv")  # snapshot #3
    write.write_batch(BATCH_4, "file_4.csv")  # snapshot #4

    # --- Query phase ---------------------------------------------------
    print("\n[2] QUERY PHASE — watch the pruning decisions\n")

    # 'Margin Call' lives in batch 3 only. Files 1, 2, 4 should be SKIPped
    # by their title ranges; only file_3.csv should be OPENed.
    query.query("title", "Margin Call")

    # 'Inception' is also in batch 3. Same pruning shape, different row.
    query.query("title", "Inception")

    # A title that doesn't exist anywhere. Every file whose range excludes it
    # is skipped; any file whose range happens to include it is opened and
    # scanned, but no rows match. (Pruning is conservative: it never skips a
    # file that *might* contain the value.)
    query.query("title", "Casablanca")

    # Filter on a different column entirely — the same min/max machinery works
    # for years. Only files whose [min year .. max year] spans 2016 are opened.
    query.query("year", 2016)

    # --- LIKE queries: the limits of min/max pruning -------------------
    print("\n[3] LIKE PHASE — prefix patterns prune, leading wildcards can't\n")

    # 'I%' is a PREFIX pattern, which is really the range ['I', 'J'). The same
    # min/max test applies, so files whose title range misses that band get
    # skipped. Only file_3 (Inception..Moonlight) can hold an 'I...' title.
    query.query("title", "I%", op="LIKE")

    # '%ll' has a LEADING wildcard. There's no lower/upper bound to compare
    # against, so min/max proves nothing and EVERY file must be opened and
    # scanned. Watch: zero files pruned. This is exactly the case real Iceberg
    # leans on bloom filters (or a full scan) to handle.
    query.query("title", "%ll", op="LIKE")

    # --- Add a row: an append is just a new snapshot -------------------
    print("\n[4] ADD A ROW — appending never rewrites old files\n")
    # In Iceberg you don't edit an existing data file; you write a NEW one and
    # record a NEW snapshot. So 'adding a row' is just another write_batch.
    write.write_batch([{"title": "Tenet", "year": 2020, "genre": "SciFi"}], "file_5.csv")
    # It's immediately visible to new queries via the latest snapshot...
    query.query("title", "Tenet")

    # --- Time travel ---------------------------------------------------
    print("\n[5] TIME TRAVEL — query the table as it existed at snapshot #2\n")
    # At snapshot #2 only batches 1 and 2 had been written, so 'Margin Call'
    # (batch 3) did not exist yet. The query sees only two data files and
    # finds nothing — exactly what the table looked like back then. Note that
    # 'Tenet' (snapshot #5) is likewise invisible here.
    query.query("title", "Margin Call", snapshot_id=2)

    print("\nDone. Poke around data/ and manifests/ to see what got written.")


if __name__ == "__main__":
    main()
