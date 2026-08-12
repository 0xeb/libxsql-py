# Callbacks and runtime control

Scalar functions receive native SQLite values and return the same value family.
Context-aware scalar functions may issue scoped nested queries without exposing
the raw APSW connection. Aggregate classes own fresh state for each SQL
invocation.

--8<-- "examples/functions.py"

Runtime settings can be controlled through product-scoped PRAGMAs or a writable
virtual table. This makes settings discoverable through SQL while preserving
validated Python state.

--8<-- "examples/runtime_settings.py"

Long-running callback loops should cooperate with query interruption by polling
`callback_interrupted()` and returning promptly when it becomes true.

Register a scalar callback with `with_context=True` when it needs a scoped
nested lookup. Its first argument is a `FunctionContext`, which offers
`query()`, `query_each()`, and `scalar()`. The context expires when the callback
returns and cannot be retained or used as a general connection escape hatch.
