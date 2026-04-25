"""Pipeline dependency graph: define run-order constraints and detect cycles."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class DependencyGraph:
    """Adjacency list representing pipeline dependencies."""
    edges: Dict[str, List[str]] = field(default_factory=dict)  # name -> [depends_on]


def build_graph(pipelines: List[dict]) -> DependencyGraph:
    """Build a DependencyGraph from a list of pipeline config dicts."""
    graph = DependencyGraph()
    for p in pipelines:
        name = p["name"]
        deps = p.get("depends_on") or []
        graph.edges[name] = list(deps)
    return graph


def detect_cycle(graph: DependencyGraph) -> Optional[List[str]]:
    """Return the first cycle found as an ordered list of names, or None."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph.edges}
    parent: Dict[str, Optional[str]] = {n: None for n in graph.edges}

    def dfs(node: str) -> Optional[List[str]]:
        color[node] = GRAY
        for neighbour in graph.edges.get(node, []):
            if neighbour not in color:
                # dependency references unknown pipeline — skip cycle check
                continue
            if color[neighbour] == GRAY:
                # reconstruct cycle
                cycle = [neighbour, node]
                cur = node
                while parent[cur] and parent[cur] != neighbour:
                    cur = parent[cur]  # type: ignore[assignment]
                    cycle.append(cur)
                cycle.append(neighbour)
                return list(reversed(cycle))
            if color[neighbour] == WHITE:
                parent[neighbour] = node
                result = dfs(neighbour)
                if result:
                    return result
        color[node] = BLACK
        return None

    for node in list(graph.edges):
        if color[node] == WHITE:
            found = dfs(node)
            if found:
                return found
    return None


def topological_order(graph: DependencyGraph) -> List[str]:
    """Return pipeline names in a valid execution order (dependencies first).

    Raises ValueError if a cycle is detected.
    """
    cycle = detect_cycle(graph)
    if cycle:
        raise ValueError(f"Cycle detected: {' -> '.join(cycle)}")

    in_degree: Dict[str, int] = {n: 0 for n in graph.edges}
    reverse: Dict[str, List[str]] = {n: [] for n in graph.edges}
    for node, deps in graph.edges.items():
        for dep in deps:
            if dep in reverse:
                reverse[dep].append(node)
                in_degree[node] += 1

    queue: deque[str] = deque(n for n, d in in_degree.items() if d == 0)
    order: List[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for dependent in reverse[node]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    return order
