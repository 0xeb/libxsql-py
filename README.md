# libxsql for Python

> **License notice:** This is source-available software, not open-source
> software. Use, modification, and distribution are governed by the
> [Human-Origin Source License v1.0](LICENSE).

`libxsql` is the Python port of Elias Bachaalany's
[libxsql](https://github.com/0xeb/libxsql). It was ported with AI assistance
under the author's direction and retains the original project's architecture,
behavioral contract, and human-origin attribution.

The package turns ordinary Python objects and iterators into SQLite virtual
tables. It also provides typed query results, script execution, graph
algorithms, and optional HTTP query transport. APSW supplies the complete
SQLite virtual-table interface while `libxsql` provides an idiomatic, typed
Python API.

This repository is a source-available preview. The distribution metadata
deliberately prevents accidental package-index upload; install it from source.

## Requirements

- CPython 3.11 or newer with the GIL enabled
- APSW 3.53.3.1 or newer

The standard-library `sqlite3` module is not a supported backend because it
does not expose virtual-table module registration.

## Installation

Install from a source checkout:

```console
python -m pip install .
```

Optional features are installed explicitly:

```console
python -m pip install ".[thinclient]"  # HTTPX clients and Hypercorn server
python -m pip install ".[trio]"        # Trio backend for AnyIO
python -m pip install ".[clipboard]"   # --clipboard support
python -m pip install ".[all]"         # every optional feature
```

For local development with `uv`:

```console
uv sync --all-extras
uv run ruff check .
uv run pyright
uv run mypy src/libxsql examples scripts
```

## Command line

The installed `libxsql` command can query a database, start the optional HTTP
server, and report exact runtime versions:

```console
libxsql query "SELECT 40 + 2 AS answer"
libxsql query --database analysis.db --format json "SELECT * FROM findings"
libxsql version
libxsql serve --database analysis.db --bind 127.0.0.1 --port 8080
```

`serve` requires the `thinclient` extra. Add `--clipboard` to a query after
installing the `clipboard` extra. Run `libxsql --help` or
`libxsql <command> --help` for all options.

## Quick start

```python
from dataclasses import dataclass

import libxsql


@dataclass(frozen=True)
class Fruit:
    identifier: int
    name: str


fruits = [Fruit(1, "apple"), Fruit(2, "banana"), Fruit(3, "cherry")]

fruit_table = (
    libxsql.table("fruits", fruits)
    .column("id", int, attr="identifier")
    .column("name", str, attr="name")
    .build()
)

with libxsql.connect() as connection:
    connection.register(fruit_table)
    result = connection.query(
        "SELECT id, name FROM fruits WHERE id > ? ORDER BY id",
        (1,),
    )

print(result.rows)
# ((2, "banana"), (3, "cherry"))
```

`.build()` returns an immutable definition; the builder remains reusable for
creating later definitions. Registrations, caches, and other connection state
are not shared between connections.

## Async usage

Async connections use AnyIO and support both asyncio and Trio:

```python
import anyio
import libxsql


async def main() -> None:
    async with await libxsql.connect_async() as connection:
        result = await connection.query("SELECT 40 + 2")
        print(result.rows[0][0])


anyio.run(main)
```

Async callbacks and async table sources must be registered through
`AsyncConnection`. Synchronous `Connection` objects intentionally reject them.
The exported `ScalarFunction`, `AsyncScalarFunction`, and
`ContextScalarFunction` aliases distinguish ordinary callbacks, awaitable
async callbacks, and callbacks whose first argument is a scoped
`FunctionContext`.

## Cancellation and streaming

Every query surface accepts an optional cooperative cancellation predicate.
Long-running Python callbacks can poll `callback_interrupted()` so cancellation
does not have to wait for the callback to return:

```python
result = connection.query(
    "SELECT * FROM expensive_table",
    should_cancel=stop_event.is_set,
)
```

Read-only result-bearing statements preserve the valid row prefix and mark the
result as partial. Timeouts additionally set `timed_out`; a later provider
failure raises `PartialQueryError` with that immutable prefix in `.result`.
Writes always fail atomically: an interrupted `INSERT`, `UPDATE`, or `DELETE`,
including one with `RETURNING`, rolls back and exposes no provisional rows.
`QueryCancelledError` and `QueryTimeoutError` report whether the interrupted
statement was read-only. `Connection.interrupt()` and
`AsyncConnection.interrupt()` are safe cancellation entry points for another
thread.

For results too large to materialize, `stream_database_script_json()` and
`stream_database_script_ndjson()` execute and encode read-only rows one at a
time. Mutation `RETURNING` rows are held until SQLite commits the statement, so
a cancelled write never leaks output that was rolled back. Their async twins
accept an awaitable sink for natural asyncio or Trio backpressure. A sink may
return `False` to stop the query after a consumer disconnects. Sink exceptions
propagate unchanged and are never reported as SQL statement errors.

The optional HTTP transport exposes authenticated `POST /cancel` for the same
cooperative path. When an authentication token is configured, `GET /status`
shares that boundary by default; set `status_requires_auth=False` only for an
intentionally public liveness probe. Query, cancellation, and shutdown
endpoints remain protected. Each active concurrent request owns an independent
cancellation token; a cancel request targets work already active without
poisoning requests that begin later. Queue-admission deadlines stop once an
owner thread starts a request; a started mutation is allowed to finish and
returns its real result. Queue saturation, admission expiry, and shutdown are
reported distinctly as HTTP 503, 408, and 503.

## Table families

- `table()` exposes live, index-addressable Python data.
- `cached_table()` materializes an iterable for a query and supports indexed
  filtering. Query-scoped caching is the safe default; `.shared_cache()` is an
  explicit optimization. `.stateful_cache_builder()` receives isolated
  registration state for transactional adapters.
- `generator_table()` streams rows lazily and can consume query constraints,
  projections, ordering, and limits. `.row_count()` makes bare `COUNT(*)`
  constant-work without constructing or walking the generator.

All families share generic `.column()`, `.filter()`, and `.index()` methods.
Typed aliases such as `.column_int()` and `.filter_eq()` are conveniences, not
separate implementations.

Writable tables can install `TransactionHooks` for prepare, commit, rollback,
and savepoint lifecycles. The state factory runs once per registration, so a
definition remains safe to reuse across connections. The legacy `.on_commit()`
spelling remains available and maps to the fallible prepare phase.

`RuntimeSettings` is a typed boolean/integer/string registry. Its canonical
`runtime_settings(key, value, type, scope)` table stages `value` updates per
connection and honors statement atomicity, read-your-writes, commit, rollback,
and savepoints. Only the imperative `timeout_push` and `timeout_pop` operations
remain PRAGMAs. `define_sql_capabilities()` builds the sorted, read-only
`sql_capabilities(name, is_supported, notes)` discovery table.

## Development guarantees

- Fully annotated public API with a `py.typed` marker
- Ruff formatting and linting
- Strict Pyright and mypy checks
- Property-based tests across supported Windows and Linux environments
- Behavior aligned with the corresponding C++ interfaces
- Universal pure-Python wheels (`py3-none-any`)
- No implicit cross-connection state

See [the documentation index](docs/index.md) for the API model, lifecycle
rules, and async guidance. The [example catalog](examples/README.md) provides
13 focused programs plus a deterministic runner that exercises asyncio and
Trio variants.

## Attribution and permissions

Copyright (c) 2024-2026 Elias Bachaalany.

This project is a Python port of, adapted from, and substantially informed by
the original libxsql implementation. It was produced with AI assistance under
the author's direction. See [LICENSE](LICENSE) for the complete grant and
restrictions. Permission requests may be opened at
[0xeb/libxsql](https://github.com/0xeb/libxsql/issues).
