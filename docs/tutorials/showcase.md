# Store showcase

This end-to-end workflow combines:

- a live writable product table;
- a shared, indexed sales cache;
- a generator table with a hidden input;
- a deterministic scalar function;
- a view joining virtual tables;
- a transaction and a multi-statement script.

--8<-- "examples/store_showcase.py"

For script execution, atomic export, and replay in isolation, see:

--8<-- "examples/scripts_and_export.py"

The graph helpers are independent of SQLite and operate on compact integer
node identifiers:

--8<-- "examples/graph_algorithms.py"
