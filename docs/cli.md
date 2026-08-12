# Command-line interface

The `libxsql` console command is installed with the base package.

## Query

Execute SQL supplied as an argument:

```console
libxsql query "SELECT sqlite_version() AS version"
libxsql query --database data.db --format json "SELECT * FROM events"
```

Pass `-` as the SQL argument to read a script from standard input. Output
formats are `text`, `json`, `csv`, and `tsv`. The `--continue-on-error`,
`--include-sql`, and `--timeout` options map directly to `ScriptOptions`.

`--clipboard` requires `python -m pip install "libxsql[clipboard]"`.

## Serve

Install the transport dependencies, then serve a connection over loopback:

```console
python -m pip install "libxsql[thinclient]"
libxsql serve --database data.db --bind 127.0.0.1 --port 8080
```

Use `--token` before binding beyond loopback, and apply deployment-specific
network controls. A port of `0` asks the operating system for an available
port.

## Version

`libxsql version` emits machine-readable JSON containing the libxsql, APSW, and
SQLite versions. `libxsql --version` emits the concise package version.

```console
libxsql version
libxsql --version
```
