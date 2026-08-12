# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Execute a SQL script, export a table, and replay it."""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import libxsql


def main() -> None:
    """Round-trip ordinary SQLite data through a temporary SQL export."""
    script = """
        CREATE TABLE events(id INTEGER PRIMARY KEY, label TEXT NOT NULL);
        INSERT INTO events(label) VALUES ('opened'), ('indexed'), ('closed');
        SELECT id, label FROM events ORDER BY id;
    """
    with TemporaryDirectory(prefix="libxsql-example-") as temporary_directory:
        export_path = Path(temporary_directory) / "events.sql"
        with libxsql.connect() as source:
            script_result = source.run_script(script, libxsql.ScriptOptions(include_sql=True))
            source.export_tables(
                ("events",),
                export_path,
                libxsql.ExportOptions(wrap_transaction=True),
            )

        with libxsql.connect() as restored:
            restored_result = restored.run_script(export_path.read_text(encoding="utf-8"))
            row_count = cast("int", restored.scalar("SELECT count(*) FROM events"))

    print(
        f"script: statements={script_result.statement_count} rows={script_result.row_count_total}",
    )
    print(
        f"replay: success={str(restored_result.success).lower()} rows={row_count}",
    )


if __name__ == "__main__":
    main()
