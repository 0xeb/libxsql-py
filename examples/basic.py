# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Expose a live product catalog and query it with ordinary SQL."""

from dataclasses import dataclass
from typing import cast

import libxsql


@dataclass(frozen=True, slots=True)
class Product:
    """One product in the live Python catalog."""

    identifier: int
    name: str
    price: float


def main() -> None:
    """Run the synchronous product-table example."""
    products = [
        Product(1, "Apple", 1.50),
        Product(2, "Banana", 0.75),
        Product(3, "Cherry", 3.00),
        Product(4, "Date", 2.25),
        Product(5, "Elderberry", 4.50),
    ]
    definition = (
        libxsql.table("products", products)
        .column_int("id", attr="identifier")
        .column_text("name", attr="name")
        .column_double("price", attr="price")
        .index("id")
        .build()
    )

    with libxsql.connect() as connection:
        connection.register(definition)
        all_products = connection.query(
            """
            SELECT id, name, printf('%.2f', price)
            FROM products
            ORDER BY id
            """,
        )
        premium = connection.query(
            """
            SELECT name, printf('%.2f', price)
            FROM products
            WHERE price > 2.0
            ORDER BY price
            """,
        )
        statistics = connection.query(
            """
            SELECT count(*), printf('%.2f', avg(price)), printf('%.2f', max(price))
            FROM products
            """,
        ).rows[0]

    print("All products:")
    for identifier, name, price in all_products.rows:
        print(
            f"  {cast('int', identifier)} | {cast('str', name)} | ${cast('str', price)}",
        )
    print("Products over $2:")
    for name, price in premium.rows:
        print(f"  {cast('str', name)}: ${cast('str', price)}")
    count, average, maximum = cast("tuple[int, str, str]", statistics)
    print(f"Stats: count={count}, avg=${average}, max=${maximum}")


if __name__ == "__main__":
    main()
