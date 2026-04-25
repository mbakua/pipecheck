"""Tests for pipecheck.dependency."""
import pytest

from pipecheck.dependency import (
    build_graph,
    detect_cycle,
    topological_order,
)


def _p(name: str, depends_on=None) -> dict:
    cfg = {"name": name}
    if depends_on:
        cfg["depends_on"] = depends_on
    return cfg


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def test_build_graph_no_deps():
    g = build_graph([_p("a"), _p("b")])
    assert g.edges == {"a": [], "b": []}


def test_build_graph_with_deps():
    g = build_graph([_p("a"), _p("b", ["a"]), _p("c", ["a", "b"])])
    assert g.edges["b"] == ["a"]
    assert g.edges["c"] == ["a", "b"]


# ---------------------------------------------------------------------------
# detect_cycle
# ---------------------------------------------------------------------------

def test_no_cycle_returns_none():
    g = build_graph([_p("a"), _p("b", ["a"]), _p("c", ["b"])])
    assert detect_cycle(g) is None


def test_direct_cycle_detected():
    # a -> b -> a
    g = build_graph([_p("a", ["b"]), _p("b", ["a"])])
    cycle = detect_cycle(g)
    assert cycle is not None
    assert len(cycle) >= 2
    # both nodes appear in the cycle path
    assert "a" in cycle
    assert "b" in cycle


def test_indirect_cycle_detected():
    # a -> b -> c -> a
    g = build_graph([_p("a", ["c"]), _p("b", ["a"]), _p("c", ["b"])])
    cycle = detect_cycle(g)
    assert cycle is not None


def test_unknown_dependency_does_not_raise():
    # depends_on references a pipeline not in the list — should not crash
    g = build_graph([_p("a", ["missing"])])
    assert detect_cycle(g) is None


# ---------------------------------------------------------------------------
# topological_order
# ---------------------------------------------------------------------------

def test_topological_order_respects_deps():
    pipelines = [_p("c", ["b"]), _p("a"), _p("b", ["a"])]
    g = build_graph(pipelines)
    order = topological_order(g)
    assert order.index("a") < order.index("b")
    assert order.index("b") < order.index("c")


def test_topological_order_no_deps_contains_all():
    pipelines = [_p("x"), _p("y"), _p("z")]
    g = build_graph(pipelines)
    order = topological_order(g)
    assert set(order) == {"x", "y", "z"}


def test_topological_order_raises_on_cycle():
    g = build_graph([_p("a", ["b"]), _p("b", ["a"])])
    with pytest.raises(ValueError, match="Cycle detected"):
        topological_order(g)
