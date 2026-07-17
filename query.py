"""
query.py — a friendly quickstart wrapper for reading with pruning.
==================================================================

A thin front door over the `iceberg` engine's scan planner. Give it a column,
a value, and an operator; it builds the predicate, plans the scan (printing
which files are SKIPped vs OPENed and why), and returns the matching rows.

    from query import query
    query("title", "Margin Call")          # exact match
    query("title", "I", op="like")         # prefix (LIKE 'I%')
    query("year", 2014, op=">=")           # range
    query("title", "Margin Call", snapshot_id=<id>)   # time travel

See demo.py for deletes, updates, compaction, and everything else.
"""

from iceberg import expressions as E
from write import table

# Map a friendly operator string to the engine's predicate class.
OPS = {
    "==": E.Eq, "!=": E.NotEq,
    "<": E.Lt, "<=": E.LtEq, ">": E.Gt, ">=": E.GtEq,
    "like": E.StartsWith,       # prefix match, LIKE 'value%'
}


def query(column, value, op="==", snapshot_id=None):
    """Plan + run `column <op> value`, print the pruning trace, return rows."""
    expr = OPS[op](column, value)
    return table().scan(expr, snapshot_id=snapshot_id).explain()


if __name__ == "__main__":
    # `python query.py` assumes some data was written first (run demo.py).
    query("title", "Margin Call")
    query("title", "I", op="like")
