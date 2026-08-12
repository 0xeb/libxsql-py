# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""General-purpose directed graph algorithms over integer node identifiers."""

# Algorithms in this module deliberately share the graph's compact adjacency
# storage rather than copying it through the public tuple-returning accessors.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

NO_NODE = -1
"""Sentinel used when no dominator or post-dominator exists."""


class DirectedGraph:
    """A mutable directed graph over node ids ``0 <= node < node_count``.

    Parallel edges and self-loops are permitted and supported by all algorithms.
    """

    __slots__ = ("_predecessors", "_successors")

    def __init__(
        self,
        node_count: int = 0,
        edges: Iterable[tuple[int, int]] = (),
    ) -> None:
        """Create a graph.

        Args:
            node_count: Number of opaque integer nodes.
            edges: Optional ``(source, destination)`` pairs.
        """
        if node_count < 0:
            message = "node_count must be non-negative"
            raise ValueError(message)
        self._successors: list[list[int]] = [[] for _ in range(node_count)]
        self._predecessors: list[list[int]] = [[] for _ in range(node_count)]
        for source, destination in edges:
            self.add_edge(source, destination)

    @property
    def node_count(self) -> int:
        """Return the number of nodes."""
        return len(self._successors)

    def __len__(self) -> int:
        """Return the number of nodes."""
        return self.node_count

    def __iter__(self) -> Iterator[int]:
        """Iterate over node identifiers."""
        return iter(range(self.node_count))

    def add_edge(self, source: int, destination: int) -> None:
        """Add the directed edge ``source -> destination``."""
        self._validate_node(source)
        self._validate_node(destination)
        self._successors[source].append(destination)
        self._predecessors[destination].append(source)

    def successors(self, node: int) -> tuple[int, ...]:
        """Return the node's successors in insertion order."""
        self._validate_node(node)
        return tuple(self._successors[node])

    def predecessors(self, node: int) -> tuple[int, ...]:
        """Return the node's predecessors in insertion order."""
        self._validate_node(node)
        return tuple(self._predecessors[node])

    def edges(self) -> Iterator[tuple[int, int]]:
        """Iterate over directed edges in node and insertion order."""
        for source, successors in enumerate(self._successors):
            for destination in successors:
                yield source, destination

    def copy(self) -> DirectedGraph:
        """Return an independent graph copy."""
        return DirectedGraph(self.node_count, self.edges())

    def _validate_node(self, node: int) -> None:
        if not 0 <= node < self.node_count:
            message = f"node {node} is outside 0..{self.node_count - 1}"
            raise IndexError(message)


def _postorder_from(
    node_count: int,
    entry: int,
    successors: list[list[int]],
) -> list[int]:
    if not 0 <= entry < node_count:
        return []
    order: list[int] = []
    visited = [False] * node_count
    stack: list[tuple[int, int]] = [(entry, 0)]
    visited[entry] = True
    while stack:
        node, successor_index = stack[-1]
        if successor_index < len(successors[node]):
            destination = successors[node][successor_index]
            stack[-1] = (node, successor_index + 1)
            if 0 <= destination < node_count and not visited[destination]:
                visited[destination] = True
                stack.append((destination, 0))
        else:
            order.append(node)
            stack.pop()
    return order


def _immediate_dominators_over(
    node_count: int,
    entry: int,
    successors: list[list[int]],
    predecessors: list[list[int]],
) -> list[int]:
    dominators = [NO_NODE] * node_count
    if not 0 <= entry < node_count:
        return dominators
    postorder = _postorder_from(node_count, entry, successors)
    post_number = [NO_NODE] * node_count
    for index, node in enumerate(postorder):
        post_number[node] = index
    reverse_postorder = list(reversed(postorder))

    def intersect(first: int, second: int) -> int:
        while first != second:
            while post_number[first] < post_number[second]:
                first = dominators[first]
            while post_number[second] < post_number[first]:
                second = dominators[second]
        return first

    dominators[entry] = entry
    changed = True
    while changed:
        changed = False
        for node in reverse_postorder:
            if node == entry:
                continue
            new_dominator = NO_NODE
            for predecessor in predecessors[node]:
                if not 0 <= predecessor < node_count or post_number[predecessor] == NO_NODE:
                    continue
                if dominators[predecessor] == NO_NODE and predecessor != entry:
                    continue
                new_dominator = (
                    predecessor
                    if new_dominator == NO_NODE
                    else intersect(predecessor, new_dominator)
                )
            if new_dominator != NO_NODE and dominators[node] != new_dominator:
                dominators[node] = new_dominator
                changed = True
    return dominators


def immediate_dominators(graph: DirectedGraph, entry: int) -> list[int]:
    """Return each node's immediate dominator relative to ``entry``.

    The entry dominates itself. Unreachable nodes contain :data:`NO_NODE`.
    An out-of-range entry therefore yields an all-``NO_NODE`` result.
    """
    return _immediate_dominators_over(
        graph.node_count,
        entry,
        graph._successors,
        graph._predecessors,
    )


def dominator_sets(graph: DirectedGraph, entry: int) -> list[list[int]]:
    """Return sorted complete dominator sets for every node."""
    immediate = immediate_dominators(graph, entry)
    output: list[list[int]] = [[] for _ in graph]
    for node in graph:
        if immediate[node] == NO_NODE:
            continue
        current = node
        output[node].append(current)
        while current != entry:
            current = immediate[current]
            output[node].append(current)
        output[node].sort()
    return output


def immediate_post_dominators(graph: DirectedGraph) -> list[int]:
    """Return immediate post-dominators using a virtual common exit.

    Sinks and nodes whose immediate post-dominator is the virtual exit contain
    :data:`NO_NODE`. Nodes that cannot reach a sink also contain ``NO_NODE``.
    """
    count = graph.node_count
    virtual_exit = count
    reverse_successors: list[list[int]] = [[] for _ in range(count + 1)]
    reverse_predecessors: list[list[int]] = [[] for _ in range(count + 1)]
    for source in graph:
        for destination in graph._successors[source]:
            reverse_successors[destination].append(source)
            reverse_predecessors[source].append(destination)
        if not graph._successors[source]:
            reverse_successors[virtual_exit].append(source)
            reverse_predecessors[source].append(virtual_exit)
    immediate = _immediate_dominators_over(
        count + 1,
        virtual_exit,
        reverse_successors,
        reverse_predecessors,
    )
    return [NO_NODE if value in {NO_NODE, virtual_exit} else value for value in immediate[:count]]


@dataclass(frozen=True, slots=True)
class NaturalLoop:
    """A natural loop identified by a dominating back edge."""

    header: int
    latch: int
    body: tuple[int, ...]


def natural_loops(graph: DirectedGraph, entry: int) -> list[NaturalLoop]:
    """Find natural loops relative to ``entry``."""
    count = graph.node_count
    immediate = immediate_dominators(graph, entry)

    def dominates(candidate: int, node: int) -> bool:
        current = node
        while True:
            if current == candidate:
                return True
            if current == entry:
                return candidate == entry
            current = immediate[current]

    output: list[NaturalLoop] = []
    for latch in graph:
        if immediate[latch] == NO_NODE:
            continue
        for header in graph._successors[latch]:
            if not dominates(header, latch):
                continue
            in_body = [False] * count
            in_body[header] = True
            stack: list[int] = []
            if not in_body[latch]:
                in_body[latch] = True
                stack.append(latch)
            while stack:
                node = stack.pop()
                for predecessor in graph._predecessors[node]:
                    if immediate[predecessor] != NO_NODE and not in_body[predecessor]:
                        in_body[predecessor] = True
                        stack.append(predecessor)
            body = tuple(node for node, included in enumerate(in_body) if included)
            output.append(NaturalLoop(header=header, latch=latch, body=body))
    return output


def strongly_connected_components(graph: DirectedGraph) -> list[int]:
    """Return a Tarjan component id for every node.

    Component ids are assigned in Tarjan discovery-completion order.
    """
    count = graph.node_count
    component = [NO_NODE] * count
    index = [NO_NODE] * count
    low_link = [0] * count
    on_stack = [False] * count
    component_stack: list[int] = []
    next_index = 0
    next_component = 0
    work: list[tuple[int, int]] = []

    for root in graph:
        if index[root] != NO_NODE:
            continue
        work.append((root, 0))
        while work:
            node, successor_index = work[-1]
            if successor_index == 0 and index[node] == NO_NODE:
                index[node] = next_index
                low_link[node] = next_index
                next_index += 1
                component_stack.append(node)
                on_stack[node] = True
            successors = graph._successors[node]
            if successor_index < len(successors):
                destination = successors[successor_index]
                work[-1] = (node, successor_index + 1)
                if index[destination] == NO_NODE:
                    work.append((destination, 0))
                elif on_stack[destination]:
                    low_link[node] = min(low_link[node], index[destination])
                continue
            if low_link[node] == index[node]:
                while True:
                    member = component_stack.pop()
                    on_stack[member] = False
                    component[member] = next_component
                    if member == node:
                        break
                next_component += 1
            work.pop()
            if work:
                parent = work[-1][0]
                low_link[parent] = min(low_link[parent], low_link[node])
    return component


def topological_order(graph: DirectedGraph) -> list[int] | None:
    """Return a deterministic topological order, or ``None`` for a cyclic graph."""
    in_degree = [len(graph._predecessors[node]) for node in graph]
    ready = [node for node, degree in enumerate(in_degree) if degree == 0]
    heapq.heapify(ready)
    order: list[int] = []
    while ready:
        node = heapq.heappop(ready)
        order.append(node)
        for destination in graph._successors[node]:
            in_degree[destination] -= 1
            if in_degree[destination] == 0:
                heapq.heappush(ready, destination)
    return order if len(order) == graph.node_count else None


__all__ = [
    "NO_NODE",
    "DirectedGraph",
    "NaturalLoop",
    "dominator_sets",
    "immediate_dominators",
    "immediate_post_dominators",
    "natural_loops",
    "strongly_connected_components",
    "topological_order",
]
