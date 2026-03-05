import pytest
from pathlib import Path

from depio.Pipeline import Pipeline
from depio.Task import Task


def test_visualize_simple(tmp_path, pipeline=None):
    # ensure graphviz is installed
    graphviz = pytest.importorskip("graphviz")

    # create simple linear pipeline a -> b
    def fn_a():
        pass

    def fn_b():
        pass

    p = Pipeline(None, quiet=True)
    a = Task("a", fn_a)
    b = Task("b", fn_b, depends_on=[a])
    p.add_task(a)
    p.add_task(b)

    # call visualize without writing to disk
    dot = p.visualize()
    assert isinstance(dot, graphviz.Digraph)
    # the source should contain an edge from a to b in some form
    assert "a" in dot.source
    assert "b" in dot.source
    assert "->" in dot.source

    # orientation defaults to vertical (no rankdir directive)
    assert "rankdir" not in dot.source

    # horizontal orientation should insert the appropriate attribute
    dh = p.visualize(orientation="horizontal")
    assert "rankdir=LR" in dh.source
    # case-insensitivity also accepted
    dh2 = p.visualize(orientation="Horizontal")
    assert "rankdir=LR" in dh2.source

    # render to file; Graphviz may need the `dot` executable, so skip if
    # rendering fails with ExecutableNotFound.
    out_file = tmp_path / "pipeline_graph"
    try:
        p.visualize(filename=out_file, format="png")
    except graphviz.backend.ExecutableNotFound:
        pytest.skip("graphviz executable not available for rendering")

    # ensure the png was created
    assert out_file.with_suffix(".png").exists()
