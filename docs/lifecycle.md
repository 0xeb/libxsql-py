# Lifecycle and caching

## Ownership

A connection owns cursors, registrations, and callbacks created through it.
Closing the connection closes those children. A cursor keeps its connection
alive while SQLite can still invoke callbacks.

Closing a cursor early releases its current source row or generator. Exhausting
it has the same effect. `close()` and `aclose()` are idempotent.

When adapting an existing APSW connection with `wrap()`, ownership is explicit.
The default borrowed wrapper closes its live cursor wrappers and unregisters
only the scalar and aggregate overloads it registered itself. This includes the
default `blob_concat(value)` aggregate. Other overloads and functions installed
directly on the borrowed APSW connection remain available.

SQLite cannot remove functions while a raw APSW statement is active. In that
case `close()` or `aclose()` raises `RegistrationError`, leaves the wrapper
open, and retains only the overloads that SQLite could not remove. Close the
raw cursor and retry connection closure; overloads already removed are not
retried.

`AsyncConnection` is event-loop affine. It may be shared freely by tasks on the
event loop where it was created or wrapped, but using it outside an async
context or from another event loop raises `ThreadingError` before APSW is
called.

## Definition reuse

A built table definition is immutable and can be registered on multiple
connections. Each registration receives independent planner state, cache
contents, callback ownership, and mutation tracking.

Registration liveness follows executed schema state. Preparing a `DROP TABLE`
that is later aborted does not retire the registration, while a successful
drop does. Rolling the drop back—at transaction or savepoint scope—restores the
registration and its write-capability checks. Unregistering an already-dropped
virtual table never removes a native table that later reused the same name.

## Cached tables

Cached tables materialize once per query by default. This avoids stale data and
surprising cross-query identity.

`.shared_cache()` opts into a per-registration persistent snapshot. Use
`registration.invalidate()` whenever the backing data changes outside a
registered mutation. Successful table mutations invalidate automatically.

Shared cache construction is atomic and single-flight. A failed or cancelled
build never publishes a partial snapshot. Existing cursors retain the snapshot
they started with, while later queries observe the replacement.

## Transactions and writes

Writable definitions declare insert, delete, and update capabilities
independently. Authorization is checked when a write is prepared, including
statements that would affect zero rows.

Atomic row-update callbacks are preferred. Per-column setters are retained for
fine-grained integrations and run in schema order. If a later setter fails
after an earlier external side effect, `PartialUpdateError` reports the columns
already applied.

All three table families accept `.transaction_hooks(TransactionHooks(...))`.
The state factory is invoked once per registration. `prepare_commit` is the
fallible phase and may reject commit; `commit` and `rollback` are final
notifications whose accidental exceptions are contained because SQLite has no
reliable error channel for them. `savepoint`, `release`, and `rollback_to`
mirror SQLite nesting and may reject their operation.

Mutation tracking distinguishes a touched callback from a successful write.
That lets rollback compensate when a callback changes external state and then
fails, while prepare/commit run only after a successful write. Rolling back a
savepoint that contained the transaction's only write does not arm commit.

`.on_commit(callback)` is retained for compatibility and maps to
`prepare_commit`; new integrations should use `TransactionHooks` explicitly.
