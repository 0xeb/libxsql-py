# Tables: live, cached, generated, and writable

The table builders share one fluent column and planner vocabulary while using
different source lifetimes.

## Live objects

Use `table()` when Python already owns an indexable collection and changes
should be visible to later queries.

--8<-- "examples/basic.py"

## Cached enumeration

Use `cached_table()` when obtaining all rows is a materialization step. Add
filters for source-native lookups, or opt into a shared snapshot and invalidate
it when the backing store changes.

--8<-- "examples/cached_table.py"

--8<-- "examples/cache_lifecycle.py"

## Lazy generation and hidden inputs

Generator tables can require SQL predicates through hidden columns. SQLite's
`LIMIT` stops the iterator without materializing the remaining rows.

--8<-- "examples/generator_table.py"

## Writes

Writable columns and delete callbacks translate SQL changes back to the Python
objects. Keep write callbacks small, explicit, and transactional.

--8<-- "examples/writable_table.py"
