# libxsql example catalog

Every example is a deterministic, non-interactive program. It writes only to
standard output, cleans up its temporary resources, and exits normally. Run the
complete catalog against the package installed in the current interpreter:

```console
python scripts/run_examples.py --all
```

Select one or more examples with repeatable `--example NAME` arguments. Async
examples run under both asyncio and Trio when selected through the runner.

## Catalog

| Name | Focus | Install extras | Backends | C++ example mapping |
| --- | --- | --- | --- | --- |
| `basic` | Live product table, filtering, and aggregation | base | sync | `examples/basic_table.cpp` |
| `async_basic` | Basic AnyIO connection lifecycle | `trio` for Trio | asyncio, Trio | — |
| `cached_table` | Query-scoped cache with an optimized equality filter | base | sync | `examples/cached_table.cpp` |
| `cache_lifecycle` | Shared snapshot reuse and explicit invalidation | base | sync | — |
| `generator_table` | Lazy rows, hidden inputs, and LIMIT pushdown | base | sync | — |
| `writable_table` | Updates, deletes, and modification hooks | base | sync | `examples/writable_table.cpp` |
| `functions` | Typed scalar and aggregate SQL callbacks | base | sync | — |
| `scripts_and_export` | Multi-statement execution and SQL export replay | base | sync | — |
| `runtime_settings` | Runtime PRAGMA parsing and writable settings | base | sync | — |
| `graph_algorithms` | Dominators, natural loops, and components | base | sync | — |
| `async_tables` | Async generator tables with asyncio and Trio | `trio` for Trio | asyncio, Trio | — |
| `http_round_trip` | Loopback ASGI server and async thin client | `thinclient`, `trio` | asyncio, Trio | — |
| `store_showcase` | End-to-end store workflow across the major APIs | base | sync | — |

The complete catalog uses every optional dependency:

```console
git clone https://github.com/0xeb/libxsql-py.git
cd libxsql-py
python -m pip install ".[all]"
python scripts/run_examples.py --all
```

The `reference` entries in [`manifest.toml`](manifest.toml) identify direct
counterparts in the C++ distribution. Empty entries mean the example teaches a
Python-specific composition rather than claiming a one-to-one source mapping.
