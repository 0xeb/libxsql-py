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
scoped nested `query()`, `query_each()`, and `scalar()` helpers.

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
`define_runtime_settings_table()` exposes their stable four-column SQL shape:

```sql
SELECT key, value, type, scope FROM runtime_settings ORDER BY key;
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
