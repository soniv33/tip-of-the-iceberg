"""
iceberg.expressions — predicates that can both filter rows AND prune files.
===========================================================================

Every predicate knows how to do two things:

    evaluate(row)      -> True/False for a concrete row (used when scanning).
    can_match(metrics) -> could ANY row in a file with these stats match?
                          (used to SKIP files without opening them.)

`can_match` is Iceberg's "inclusive projection": given only a file's
min/max/null-count summary, prove whether it's worth reading. If it returns
False, the whole file is skipped. If it returns True, the file is opened and
`evaluate` is run per row.

Supported: Eq, NotEq, Lt, LtEq, Gt, GtEq, In, NotIn, IsNull, NotNull,
StartsWith, plus And / Or / Not and AlwaysTrue. `~expr` negates.
"""


def _prefix_end(prefix):
    """Smallest string strictly greater than every string starting with prefix."""
    return prefix[:-1] + chr(ord(prefix[-1]) + 1) if prefix else None


class FileMetrics:
    """A file's statistics, as seen by the pruner. Built from a manifest entry."""

    def __init__(self, entry):
        self.record_count = entry["record_count"]
        self.null_counts = entry.get("null_counts", {})
        self.lower = entry.get("lower_bounds", {})
        self.upper = entry.get("upper_bounds", {})
        self.partition = entry.get("partition", {})

    def has_bounds(self, col):
        return col in self.lower and col in self.upper

    def all_null(self, col):
        # A file where a column is entirely null has no usable min/max.
        return self.null_counts.get(col, 0) == self.record_count

    def may_contain_null(self, col):
        return self.null_counts.get(col, 0) > 0

    def has_non_null(self, col):
        return self.record_count - self.null_counts.get(col, 0) > 0


# ---------------------------------------------------------------------------
# Base classes.
# ---------------------------------------------------------------------------
class Expr:
    def evaluate(self, row):
        raise NotImplementedError

    def can_match(self, m):
        raise NotImplementedError

    def refs(self):
        return set()

    def negate(self):
        raise NotImplementedError

    def __invert__(self):
        return self.negate()


class _Unary(Expr):
    """A predicate over a single column and a literal (or nothing)."""

    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    def refs(self):
        return {self.name}

    def _cell(self, row):
        return row.get(self.name)


class AlwaysTrue(Expr):
    def evaluate(self, row):
        return True

    def can_match(self, m):
        return True

    def negate(self):
        return AlwaysFalse()

    def __repr__(self):
        return "true"


class AlwaysFalse(Expr):
    def evaluate(self, row):
        return False

    def can_match(self, m):
        return False

    def negate(self):
        return AlwaysTrue()

    def __repr__(self):
        return "false"


# ---------------------------------------------------------------------------
# Comparison predicates.
# ---------------------------------------------------------------------------
class Eq(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c == self.value

    def can_match(self, m):
        if not m.has_bounds(self.name) or m.all_null(self.name):
            return not m.has_bounds(self.name)  # no stats -> can't rule out
        return m.lower[self.name] <= self.value <= m.upper[self.name]

    def negate(self):
        return NotEq(self.name, self.value)

    def __repr__(self):
        return f"{self.name} == {self.value!r}"


class NotEq(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c != self.value

    def can_match(self, m):
        # Only impossible if the file is a single value equal to it, no nulls.
        if not m.has_bounds(self.name):
            return True
        if m.may_contain_null(self.name):
            return True
        return not (m.lower[self.name] == m.upper[self.name] == self.value)

    def negate(self):
        return Eq(self.name, self.value)

    def __repr__(self):
        return f"{self.name} != {self.value!r}"


class Lt(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c < self.value

    def can_match(self, m):
        if not m.has_bounds(self.name):
            return True
        if m.all_null(self.name):
            return False
        return m.lower[self.name] < self.value

    def negate(self):
        return GtEq(self.name, self.value)

    def __repr__(self):
        return f"{self.name} < {self.value!r}"


class LtEq(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c <= self.value

    def can_match(self, m):
        if not m.has_bounds(self.name):
            return True
        if m.all_null(self.name):
            return False
        return m.lower[self.name] <= self.value

    def negate(self):
        return Gt(self.name, self.value)

    def __repr__(self):
        return f"{self.name} <= {self.value!r}"


class Gt(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c > self.value

    def can_match(self, m):
        if not m.has_bounds(self.name):
            return True
        if m.all_null(self.name):
            return False
        return m.upper[self.name] > self.value

    def negate(self):
        return LtEq(self.name, self.value)

    def __repr__(self):
        return f"{self.name} > {self.value!r}"


class GtEq(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c >= self.value

    def can_match(self, m):
        if not m.has_bounds(self.name):
            return True
        if m.all_null(self.name):
            return False
        return m.upper[self.name] >= self.value

    def negate(self):
        return Lt(self.name, self.value)

    def __repr__(self):
        return f"{self.name} >= {self.value!r}"


class In(_Unary):
    def __init__(self, name, values):
        super().__init__(name, list(values))

    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c in self.value

    def can_match(self, m):
        if not m.has_bounds(self.name):
            return True
        if m.all_null(self.name):
            return False
        lo, hi = m.lower[self.name], m.upper[self.name]
        return any(lo <= v <= hi for v in self.value)

    def negate(self):
        return NotIn(self.name, self.value)

    def __repr__(self):
        return f"{self.name} in {self.value!r}"


class NotIn(_Unary):
    def __init__(self, name, values):
        super().__init__(name, list(values))

    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and c not in self.value

    def can_match(self, m):
        return True  # conservative: rarely prunable

    def negate(self):
        return In(self.name, self.value)

    def __repr__(self):
        return f"{self.name} not in {self.value!r}"


class IsNull(_Unary):
    def evaluate(self, row):
        return self._cell(row) is None

    def can_match(self, m):
        if self.name not in m.null_counts:
            return True
        return m.may_contain_null(self.name)

    def negate(self):
        return NotNull(self.name)

    def __repr__(self):
        return f"{self.name} is null"


class NotNull(_Unary):
    def evaluate(self, row):
        return self._cell(row) is not None

    def can_match(self, m):
        if self.name not in m.null_counts:
            return True
        return m.has_non_null(self.name)

    def negate(self):
        return IsNull(self.name)

    def __repr__(self):
        return f"{self.name} is not null"


class StartsWith(_Unary):
    """SQL `col LIKE 'prefix%'`. A prefix is a range, so it still prunes."""

    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and str(c).startswith(self.value)

    def can_match(self, m):
        if not m.has_bounds(self.name):
            return True
        if m.all_null(self.name):
            return False
        lo, hi = str(m.lower[self.name]), str(m.upper[self.name])
        return not (hi < self.value or lo >= _prefix_end(self.value))

    def negate(self):
        return NotStartsWith(self.name, self.value)

    def __repr__(self):
        return f"{self.name} like '{self.value}%'"


class NotStartsWith(_Unary):
    def evaluate(self, row):
        c = self._cell(row)
        return c is not None and not str(c).startswith(self.value)

    def can_match(self, m):
        return True  # a leading NOT-prefix can't be pruned

    def negate(self):
        return StartsWith(self.name, self.value)

    def __repr__(self):
        return f"{self.name} not like '{self.value}%'"


# ---------------------------------------------------------------------------
# Boolean combinators.
# ---------------------------------------------------------------------------
class And(Expr):
    def __init__(self, *children):
        self.children = children

    def evaluate(self, row):
        return all(c.evaluate(row) for c in self.children)

    def can_match(self, m):
        # A file can match only if EVERY conjunct could match it.
        return all(c.can_match(m) for c in self.children)

    def refs(self):
        return set().union(*(c.refs() for c in self.children))

    def negate(self):
        return Or(*(c.negate() for c in self.children))

    def __repr__(self):
        return "(" + " and ".join(map(repr, self.children)) + ")"


class Or(Expr):
    def __init__(self, *children):
        self.children = children

    def evaluate(self, row):
        return any(c.evaluate(row) for c in self.children)

    def can_match(self, m):
        # A file can match if ANY disjunct could match it.
        return any(c.can_match(m) for c in self.children)

    def refs(self):
        return set().union(*(c.refs() for c in self.children))

    def negate(self):
        return And(*(c.negate() for c in self.children))

    def __repr__(self):
        return "(" + " or ".join(map(repr, self.children)) + ")"


def Not(expr):
    """Logical negation, pushed down to the leaves (De Morgan)."""
    return expr.negate()
