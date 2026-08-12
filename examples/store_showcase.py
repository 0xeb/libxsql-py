# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Combine the major libxsql features in one deterministic store workflow."""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

import libxsql


@dataclass(slots=True)
class Product:
    """One mutable store product."""

    identifier: int
    name: str
    category: str
    price: float
    stock: int


@dataclass(frozen=True, slots=True)
class Sale:
    """One immutable sale record."""

    product_id: int
    quantity: int


def main() -> None:
    """Run a complete in-memory reporting and mutation workflow."""
    products = [
        Product(1, "Apple", "fruit", 1.50, 8),
        Product(2, "Banana", "fruit", 0.75, 5),
        Product(3, "Coffee", "pantry", 8.00, 3),
    ]
    sales = (Sale(1, 2), Sale(2, 4), Sale(3, 1))

    def with_tax(price: float) -> float:
        return round(price * 1.10, 2)

    def set_stock(product: Product, value: libxsql.SQLiteValue) -> bool:
        if not isinstance(value, int) or value < 0:
            return False
        product.stock = value
        return True

    products_definition = (
        libxsql.table("products", products)
        .column_int("id", attr="identifier")
        .column_text("name", attr="name")
        .column_text("category", attr="category")
        .column_double("price", attr="price")
        .column("stock", int, attr="stock", set=set_stock)
        .index("id")
        .build()
    )
    sales_definition = (
        libxsql.cached_table("sales", lambda: sales)
        .column_int("product_id", attr="product_id")
        .column_int("quantity", attr="quantity")
        .index("product_id")
        .shared_cache()
        .build()
    )

    def in_stock(values: tuple[libxsql.SQLiteValue, ...]) -> Iterator[Product]:
        minimum = values[0]
        if not isinstance(minimum, int):
            return
        yield from (product for product in products if product.stock >= minimum)

    def all_products() -> tuple[Product, ...]:
        return tuple(products)

    stocked_definition = (
        libxsql.generator_table("stocked", all_products)
        .column_int("id", attr="identifier")
        .column_text("name", attr="name")
        .column_int("stock", attr="stock")
        .hidden_column_int("minimum")
        .parametric_filter(("minimum",), in_stock, estimated_rows=3)
        .build()
    )

    with libxsql.connect() as connection:
        connection.register(products_definition)
        connection.register(sales_definition)
        connection.register(stocked_definition)
        connection.register_function(
            "with_tax",
            with_tax,
            1,
            deterministic=True,
        )
        connection.run_script(
            """
            CREATE TEMP VIEW sales_report AS
                SELECT p.name, s.quantity, printf('%.2f', p.price * s.quantity) AS total
                FROM products AS p
                JOIN sales AS s ON s.product_id = p.id;
            CREATE TABLE audit(message TEXT NOT NULL);
            INSERT INTO audit VALUES ('inventory adjusted');
            """,
        )
        with connection.transaction():
            connection.execute("UPDATE products SET stock = stock - 2 WHERE id = 1")

        report = connection.query(
            "SELECT name, quantity, total FROM sales_report ORDER BY name",
        )
        available = connection.query(
            "SELECT name FROM stocked WHERE minimum = 4 ORDER BY id",
        )
        taxed = cast(
            "str",
            connection.scalar(
                "SELECT printf('%.2f', with_tax(price)) FROM products WHERE id = 3",
            ),
        )
        audit_count = cast("int", connection.scalar("SELECT count(*) FROM audit"))

    sales_text = ",".join(
        f"{cast('str', row[0])}:{cast('int', row[1])}@{cast('str', row[2])}" for row in report.rows
    )
    print("sales=" + sales_text)
    print("stocked=" + ",".join(str(row[0]) for row in available.rows))
    print(f"coffee-with-tax={taxed}")
    print(f"audit-rows={audit_count}")


if __name__ == "__main__":
    main()
