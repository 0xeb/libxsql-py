# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Analyze a deterministic control-flow graph."""

import libxsql


def main() -> None:
    """Compute dominators, loops, and strongly connected components."""
    graph = libxsql.DirectedGraph(
        5,
        (
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 1),
            (3, 4),
        ),
    )
    dominators = libxsql.immediate_dominators(graph, 0)
    loops = libxsql.natural_loops(graph, 0)
    components = libxsql.strongly_connected_components(graph)

    print("idom=" + ",".join(str(node) for node in dominators))
    print("loops=" + ";".join(f"{loop.header}:{','.join(map(str, loop.body))}" for loop in loops))
    print("scc=" + ",".join(str(component) for component in components))


if __name__ == "__main__":
    main()
