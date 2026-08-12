# libxsql documentation

`libxsql` exposes Python data through SQLite without requiring callers to write
APSW virtual-table callbacks directly.

## Guides

- [Table tutorial](tutorials/tables.md): live, cached, generated, and writable tables
- [Callbacks and runtime](tutorials/callbacks-runtime.md): functions and settings
- [Async tutorial](tutorials/async.md): asyncio/Trio queries and row sources
- [Transport tutorial](tutorials/transport.md): a complete loopback HTTP round trip
- [Store showcase](tutorials/showcase.md): the major APIs in one workflow
- [API model](api.md): connections, results, builders, registration, and values
- [Async model](async.md): asyncio/Trio operation and callback rules
- [Lifecycle and caching](lifecycle.md): ownership, close ordering, and cache scope

The [public example catalog](https://github.com/0xeb/libxsql-py/tree/main/examples)
contains 13 focused, directly executable programs. Its runner expands the three
async programs across asyncio and Trio for 16 deterministic execution cases.

## Design principles

1. Python values remain Python values. SQL NULL maps to `None`; integers,
   floats, text, and blobs map to their obvious built-ins.
2. Builders validate eagerly and definitions are immutable.
3. Connection-local state never leaks through a reusable definition.
4. Callback exceptions retain their Python traceback and cause.
5. Resource cleanup is explicit, deterministic, and also safe under garbage
   collection.
6. The APSW connection is available as an escape hatch, but no public API
   requires direct APSW callback knowledge.

## Stability

Version 0.0.x is a source-available preview. Public names and behavior are typed and
tested, but compatibility is not promised until the first stable release.
