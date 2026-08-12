# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Update and delete rows in a live virtual table."""

from dataclasses import dataclass
from typing import cast

import libxsql


@dataclass(slots=True)
class Task:
    """One mutable task."""

    identifier: int
    title: str
    done: bool = False


def main() -> None:
    """Apply SQL writes to Python task objects."""
    tasks = [
        Task(1, "Write documentation"),
        Task(2, "Fix bug #123"),
        Task(3, "Review PR", done=True),
        Task(4, "Deploy to staging"),
    ]
    modifications: list[str] = []

    def set_title(task: Task, value: libxsql.SQLiteValue) -> bool:
        if not isinstance(value, str):
            return False
        task.title = value
        return True

    def set_done(task: Task, value: libxsql.SQLiteValue) -> bool:
        if not isinstance(value, int):
            return False
        task.done = bool(value)
        return True

    def delete(index: int) -> bool:
        del tasks[index]
        return True

    definition = (
        libxsql.table("tasks", tasks)
        .on_modify(modifications.append)
        .column_int("id", attr="identifier")
        .column("title", str, attr="title", set=set_title)
        .column("done", int, get=lambda task: int(task.done), set=set_done)
        .deletable(delete)
        .build()
    )

    with libxsql.connect() as connection:
        connection.register(definition)
        connection.execute("UPDATE tasks SET done = 1 WHERE id = 2")
        connection.execute("UPDATE tasks SET title = 'Write README.md' WHERE id = 1")
        connection.execute("DELETE FROM tasks WHERE id = 3")
        result = connection.query(
            "SELECT id, title, done FROM tasks ORDER BY id",
        )

    print("tasks:")
    for identifier, title, done in result.rows:
        marker = "x" if done == 1 else " "
        print(f"  [{marker}] {cast('int', identifier)}: {cast('str', title)}")
    print("hooks=" + ",".join(modifications))


if __name__ == "__main__":
    main()
