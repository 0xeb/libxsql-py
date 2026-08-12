# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Parse runtime PRAGMAs and expose settings through a writable table."""

from typing import cast

import libxsql


def main() -> None:
    """Apply a product-scoped PRAGMA, then update the same settings via SQL."""
    settings = libxsql.RuntimeSettings()
    request = libxsql.parse_runtime_pragma(
        "-- tune this query\nPRAGMA libxsql.timeout_push = 1250;",
        "libxsql",
    )
    reply = libxsql.handle_common_runtime_pragma(request, "libxsql", settings)

    with libxsql.connect() as connection:
        connection.register(libxsql.define_runtime_settings_table(settings))
        connection.execute(
            "UPDATE runtime_settings SET value='8' WHERE key='max_queue'",
        )
        rows = connection.query(
            """
            SELECT key, value
            FROM runtime_settings
            WHERE key IN ('query_timeout_ms', 'max_queue')
            ORDER BY key
            """,
        )

    print(f"pragma: handled={str(reply.handled).lower()} value={reply.value}")
    for key, value in rows.rows:
        print(f"{cast('str', key)}={cast('str', value)}")


if __name__ == "__main__":
    main()
