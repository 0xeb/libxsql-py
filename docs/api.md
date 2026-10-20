# API model

## Connections and cursors

`connect()` creates a synchronous `Connection`; `connect_async()` creates an
`AsyncConnection`. Both expose execution, query, scalar, transaction, function,
aggregate, script, and virtual-table registration methods.

`Connection.wrap()` adapts an existing idle APSW connection. The wrapper does
not close a borrowed connection unless `owns=True` is specified.

`execute()` returns a streaming cursor. `query()` materializes an immutable
`QueryResult` with `columns`, `rows`, elapsed time, timeout state, and warnings.
`scalar()` returns the first column of the first row or a caller-supplied
default.

The API is deliberately familiar to users of Python database drivers, but it
does not claim strict PEP 249 conformance.

## Values

`SQLiteValue` is the native result value type, while `SQLiteBindable` adds the
writable buffer types accepted as statement inputs:

```python
SQLiteValue = int | float | str | bytes | None
SQLiteBindable = SQLiteValue | bytearray | memoryview
```

Writable buffers are copied to `bytes`. Integers must fit SQLite's signed
64-bit range. Datetimes, decimals, enums, and application-specific objects
require explicit adapters.

Bindings are a sequence or a mapping. A sequence binds every placeholder
spelling (`?`, `?NNN`, `:name`, `@name`, `$name`) by position; a mapping binds
named placeholders by name. Text binds by its length, so an embedded NUL is
kept, and an empty `bytes` stays a blob rather than NULL. A binding count that
differs from the statement's raises `apsw.BindingsError` before the statement
runs. Result cells come back as native values, so each cell's type is its
SQLite storage class.

`await AsyncConnection.changes()` and `await AsyncConnection.total_changes()`
read counters on the owning worker.

`Connection.changes` reports the rows the last statement changed;
`Connection.total_changes` reports every row changed since the connection
opened, virtual-table writes included.

A scalar function is not deterministic unless it is registered with
`deterministic=True`. SQLite may evaluate a deterministic call with constant
arguments once and reuse the value, which is wrong for a function with side
effects; only a deterministic function may appear in an index expression.

## Table definitions

The three factories `table()`, `cached_table()`, and `generator_table()` return
fluent builders. The canonical column method is:

```python
builder.column(
    name,
    python_type,
    *,
    get=None,
    attr=None,
    key=None,
    set=None,
    nullable=False,
    hidden=False,
)
```

At most one extraction strategy may be supplied. With none, rows are read by
the column name as either a mapping key or an attribute. Typed methods such as
`column_int()` and `column_text()` are aliases.

`.build()` validates duplicate names, source compatibility, row access,
nullability, setter configuration, constraints, and indexes. It returns an
immutable definition that can be registered more than once.

Generator builders can push down more than equality. `.filter_like_text()`
claims a `LIKE` or `GLOB` whose pattern starts with a literal prefix (a leading
`%`, `_`, or `\` falls back to the scan), and SQLite re-checks every row.
`.constraint_filter_projection()`, `.filter_eq_projection()`, and
`.parametric_filter_projection()` also pass the factory the query's
`Projection`, so it can skip computing columns nobody selected.

An equality filter (`.filter_eq()`, `.filter_eq_projection()`, or `.filter()`
with `EQ`) is exact: SQLite does not re-check its rows, so the factory receives
the value as the column compares it. Numeric text becomes a number for an
`INTEGER` or `REAL` column and a number becomes text for a `TEXT` column, as
SQLite's column affinity would make it; an `INTEGER` column receives an `int`.
When no row of the column's type can equal the value (`NULL`, `10.5` for an
`INTEGER` column, a blob for a `TEXT` column), the factory is not called and
the plan returns no rows. `index()` lookups, the rowid lookup, and
`.constraint_cache_filter()` compare the same way. Range filters, constraint
filters, and parametric filters pass the value exactly as SQLite supplied it.

Cached builders accept `.constraint_filter(specs, factory)` and
`.constraint_cache_filter(specs)`, which consume several constraints in one
plan, for example `lo <= ? AND hi >= ?` in an interval-containment join. The
cache variant evaluates the comparisons in memory against each column's
`index()` key.

On every table family the filters compete on their estimated cost. A
constraint filter, and a parametric filter (a constraint filter whose inputs
are all equalities), is chosen only when it is strictly cheaper than the best
usable single-column filter (an equality, or a `LIKE`/`GLOB` with a literal
prefix) and, on a shared cache, than an equality `index()` lookup priced at
1.0. So `name LIKE 'p%' AND addr > ?` runs a cheap `LIKE` filter rather than a
costly address range, and a single-column filter wins a tie.

A value written to an `INTEGER` column is exact in the same way. SQLite applies
no column affinity to the values an `INSERT` or `UPDATE` hands the table, so
the insert callback, the column setter, and the `.updatable()` changes receive
the `int` the value equals as `exact_int64()` defines it: `16`, `16.0`, `'16'`,
and `' 16 '` all arrive as `16`. A non-integral or out-of-range real,
non-integer text such as `'0x10'`, a blob, and `NULL` for a column that is not
`nullable` fail the statement with
`column "<name>": expected an integer, got ...`, and no callback runs.

Every write callback receives the row the statement's rowid resolves to: the
column setters, the `.deletable()` callback, and the `.updatable()` callback,
which is called as `callback(row, changes)` with only the changed columns. A
rowid that resolves to no row fails the statement with
`row <id> not found in <table>` before any callback runs. How a rowid resolves
depends on the table:

- **With a row lookup.** `.rowid()` reports a key from every plan (full scans,
  index lookups and filters alike), and `.row_lookup()` turns the key back into
  a row, or returns `None`. The row lookup is then the only resolver, also for
  `WHERE rowid = ?`; a key it cannot resolve is never retried as a position,
  which would name a different row. Keys may be negative (an unsigned address
  at or above 2**63). `.build()` rejects `.row_lookup()` or
  `.stateful_row_lookup()` on a cached table without `.rowid()`.
- **A cached table without a row lookup** is written by cache position. A
  shared cache resolves the position in its cached rows, and after a write has
  invalidated them the rest of the statement still resolves in the rows it
  scanned. A query-scoped table snapshots its rows at the statement's first
  write, so a multi-row UPDATE or DELETE that shifts or reorders the source
  still reaches exactly the rows it matched; the next scan drops the snapshot.
  A `.rowid()` key is not a position, so on such a table a DELETE fails with
  `row <id> not found in <table>` and an UPDATE is read-only.
- **A live `table()`** resolves a position to the row the statement scanned
  there, through `.row_at()` or its row sequence. A delete callback removes its
  row and moves every later row up one, so the table keeps a per-statement
  record of the positions the statement deleted and reads the row at the
  position minus the deleted positions before it: deleting positions 0, 2 and
  3 of four rows removes exactly those rows. The next scan drops the record,
  and a position the statement already deleted names no row. A writable live
  table with `.rowid()` must have a row lookup, since its rowids are then keys.

Writable tables with a filter factory (`.filter_eq()`, `.filter()`, or
`.constraint_filter()`) need `.rowid()`: a factory's rows are positions in its
own output, not in the table.

```python
names = (
    cached_table("names", collect_names)
    .rowid(lambda row: row.addr)
    .row_lookup(lookup_name)  # addr -> row, or None
    .column_int("addr")
    .column("name", str, set=rename)
    .build()
)
```

A multi-constraint or parametric factory receives each constraint value exactly as SQLite passed it, without
the column's affinity (`k = '10'` arrives as text, `k = 10.5` as a float), and
SQLite trusts the rows it returns. Compare key values with the helpers rather
than `int(value)`: `exact_int64(value)` is the integer an INTEGER column must
equal (or `None`: yield no rows), `exact_text(value)` the text a TEXT column
must equal, and `Int64KeyRange` narrows an inclusive integer window with each
`constrain(constraint.op, constraint.value)`, so `k >= 14.5` keeps 15 and a
NULL bound matches nothing. When the key is really an unsigned 64-bit value
stored bit-cast (an address), keys >= 2**63 are negative in SQL and sort first;
`Int64KeyRange.unsigned_spans(descending=...)` returns the window as
`U64Span(lo, hi)` values in SQL order (the high half first when it crosses
zero), which is the order a walker that claims `ORDER BY` must visit them in.

Generator builders expose `.row_count(callback)` for an exact bare `COUNT(*)`
plan. SQLite then obtains the count without constructing or advancing the
generator; `COUNT(column)` still scans because it must preserve NULL semantics.

`define_sql_capabilities()` builds the canonical, sorted, read-only
`sql_capabilities(name, is_supported, notes)` table. Empty and duplicate names
are rejected while building the definition.

## Registration

`connection.register(definition)` creates a temporary, connection-local table
by default and returns a `Registration`. Keeping the registration in a context
manager gives deterministic unregistration:

```python
with connection.register(definition) as registration:
    rows = connection.query("SELECT * FROM my_table").rows
```

The connection itself also owns active registrations, so discarding the handle
does not invalidate a live SQLite module accidentally.

`ScalarFunction` describes synchronous scalar callbacks.
`AsyncScalarFunction` additionally permits an awaitable result and is accepted
by `AsyncConnection`. `ContextScalarFunction` requires a leading
`FunctionContext`, which is valid only for that callback invocation and offers
scoped nested `query()`, `query_each()`, and `scalar()` helpers, plus
`invalidate_cached_tables()` for a callback that changes the data the tables
read.

`Connection.invalidate_cached_tables()`, `AsyncConnection.invalidate_cached_tables()`
and `libxsql.vtable.invalidate_cached_tables(raw_connection)` invalidate every
table registered on one SQLite connection, including tables registered with
`libxsql.vtable.register_definition()` whose handle was discarded.

`Connection.vtab_write_count`, `AsyncConnection.vtab_write_count` and
`libxsql.vtable.vtab_write_count(raw_connection)` count the row writes the
connection's virtual tables accepted: every INSERT, UPDATE or DELETE a table's
callback carried out. Reads, refused writes, and temp or ordinary SQLite tables
never count, and a later `ROLLBACK` does not lower the count, because SQLite
does not undo a host write. Compare two readings to ask whether a host-backed
table changed. A session or bookkeeping table opts out with
`.counts_as_write(enabled=False)`; the `runtime_settings` table does.

## Errors

All library-owned errors derive from `LibxsqlError`. Errors representing
configuration, lifecycle, threading, registration, read-only access, full
scans, timeout, partial queries, partial updates, protocol failures, and
unsupported runtimes have distinct types.

Exceptions raised by user callbacks are not flattened into generic database
errors. Their original type and traceback remain available.
`QueryCancelledError` and `QueryTimeoutError` retain `result_columns` and a
`readonly` classification when SQLite prepared a statement before it was
interrupted; `QueryTimeoutError` also retains an optional `elapsed_ms`.
`PartialQueryError.result` carries the immutable read-only prefix produced
before a later provider failure. Interrupted writes, including DML with
`RETURNING`, roll back and raise a cancellation or timeout error instead of
claiming a partial result.

## Runtime settings

`RuntimeSettings` registers boolean, bounded integer, and string settings.
`define_runtime_settings_table()` exposes their stable six-column SQL shape;
`kind` is `value` or `action`, and `settable` says whether an `UPDATE` of that
row is accepted:

```sql
SELECT key, value, type, scope, kind, settable FROM runtime_settings ORDER BY key;
UPDATE runtime_settings SET value = '2500'
WHERE key = 'query_timeout_ms';
```

SQL updates are connection-local until commit. Reads on that connection see
the staged canonical value; other connections see committed state. Rollback,
savepoint rollback, and a failed multi-row statement discard the appropriate
overlay. Direct Python setters are immediate writes and are never replayed by a
later SQL rollback. Value-setting PRAGMAs are intentionally unsupported; only
`timeout_push` and `timeout_pop` remain imperative PRAGMA operations.

## Script results and bounded streaming

`run_database_script()` and `run_database_script_async()` materialize a
canonical `ScriptResult`. Use the direct database stream functions when the
number of rows is not bounded:

```python
chunks: list[str] = []
stream_database_script_json(
    connection,
    "SELECT * FROM events;",
    ScriptOptions(),
    chunks.append,
)
```

`stream_database_script_json()` emits the canonical envelope incrementally.
`stream_database_script_ndjson()` emits one column-keyed object per row. Both
have `_async` twins whose sink may be an ordinary callable or an awaitable
callable. Returning `False` from any direct-stream sink stops execution. A sink
exception propagates unchanged rather than being encoded as a statement error.

`script_result_to_json()` and `stream_script_result_json()` operate on an
already materialized result and therefore do not provide the same memory
bound.
