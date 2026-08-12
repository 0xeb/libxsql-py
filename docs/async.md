# Async operation

`AsyncConnection` uses APSW's async controller and AnyIO. The same API is
available under asyncio and Trio; there is no asyncio-only public object.

```python
async with await libxsql.connect_async("data.db") as connection:
    await connection.execute("CREATE TABLE example(value)")
    await connection.executemany(
        "INSERT INTO example VALUES (?)",
        [(1,), (2,), (3,)],
    )
    result = await connection.query("SELECT sum(value) FROM example")
```

SQLite work runs in the connection's dedicated worker. Awaiting an async user
callback routes execution back to the owning event loop. Attempting to use the
same connection recursively from such a callback raises `ReentrancyError`
instead of deadlocking.

Async table sources may be async iterators. Cancellation closes their cursors
and awaits `aclose()` when provided. Synchronous iterators are also supported
and run with the same lifecycle guarantees.

An `AsyncConnection` belongs to the AnyIO event-loop context in which it was
opened. Moving it between event loops raises `ThreadingError`. Independent
connections can run concurrently.

HTTP support uses an ASGI application, HTTPX clients, and Hypercorn serving.
The server API does not expose framework-specific request or response objects.
