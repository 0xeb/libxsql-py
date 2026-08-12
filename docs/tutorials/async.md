# Async connections and tables

`AsyncConnection` owns one SQLite worker and presents the same API under
asyncio and Trio. The example runner executes each program under both backends.

## Queries

--8<-- "examples/async_basic.py"

## Async row sources

Async iterators can be registered only through `AsyncConnection`. Their
iteration and cleanup are routed back to the owning event loop.

--8<-- "examples/async_tables.py"
