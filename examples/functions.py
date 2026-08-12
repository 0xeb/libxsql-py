# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Register typed scalar and aggregate SQL callbacks."""

from typing import cast

import libxsql


class ProductTotal:
    """Accumulate integer products across one aggregate invocation."""

    def __init__(self) -> None:
        """Initialize the multiplicative identity."""
        self.value = 1

    def step(self, value: int | None) -> None:
        """Multiply one non-NULL input into the aggregate."""
        if value is not None:
            self.value *= value

    def final(self) -> int:
        """Return the accumulated product."""
        return self.value


def main() -> None:
    """Call custom scalar and aggregate functions from SQL."""

    def label(value: int) -> str:
        return f"item-{value:02d}"

    def nested_label(context: libxsql.FunctionContext, value: int) -> str:
        return cast("str", context.scalar("SELECT item_label(?)", (value,)))

    with libxsql.connect() as connection:
        connection.register_function("item_label", label, 1, deterministic=True)
        connection.register_function(
            "nested_label",
            nested_label,
            1,
            deterministic=True,
            with_context=True,
        )
        connection.register_aggregate("product_total", ProductTotal, 1)
        result = connection.query(
            """
            SELECT nested_label(7), product_total(value)
            FROM (
                SELECT 2 AS value
                UNION ALL SELECT 3
                UNION ALL SELECT 7
            )
            """,
        )

    label_value, product = cast("tuple[str, int]", result.rows[0])
    print(f"label={label_value}")
    print(f"product={product}")


if __name__ == "__main__":
    main()
