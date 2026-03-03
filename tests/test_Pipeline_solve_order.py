"""
Tests for Pipeline._solve_order(): DAG resolution, path vs task
dependencies, backlinks, deduplication, and missing-dependency errors.

IMPORTANT: Each Task must use a *distinct* function object, because Task
equality is based on (func, cleaned_args). Two tasks that share the same
function and the same args are considered equal and the second one is
silently dropped by add_task(). Using per-test nested functions avoids
accidental deduplication.
"""
import pytest
from pathlib import Path

from depio.Pipeline import Pipeline
from depio.Task import Task
from depio.exceptions import DependencyNotAvailableException


@pytest.fixture
def pipeline():
    return Pipeline(None, quiet=True)


# ---------------------------------------------------------------------------
# Single task with no dependencies
# ---------------------------------------------------------------------------

def test_solve_order_no_deps(pipeline):
    def fn():
        pass

    task = Task("t", fn)
    pipeline.add_task(task)
    pipeline._solve_order()

    assert task.path_dependencies == []
    assert task.task_dependencies == []


# ---------------------------------------------------------------------------
# Existing path dependency → goes into path_dependencies
# ---------------------------------------------------------------------------

def test_solve_order_existing_path_dep(tmp_path, pipeline):
    dep = tmp_path / "dep.txt"
    dep.write_text("data")

    def fn():
        pass

    task = Task("t", fn, depends_on=[dep])
    pipeline.add_task(task)
    pipeline._solve_order()

    assert dep in task.path_dependencies
    assert task.task_dependencies == []


# ---------------------------------------------------------------------------
# Non-existing path dep NOT produced by any task → raises
# ---------------------------------------------------------------------------

def test_solve_order_missing_path_dep_raises(tmp_path, pipeline):
    missing = tmp_path / "missing.txt"

    def fn():
        pass

    task = Task("t", fn, depends_on=[missing])
    pipeline.add_task(task)

    with pytest.raises(DependencyNotAvailableException):
        pipeline._solve_order()


# ---------------------------------------------------------------------------
# Path produced by another task → becomes a task_dependency
# ---------------------------------------------------------------------------

def test_solve_order_path_produced_by_task_becomes_task_dep(tmp_path, pipeline):
    out = tmp_path / "out.txt"

    def producer():
        pass

    def consumer():
        pass

    task_a = Task("producer", producer, produces=[out])
    task_b = Task("consumer", consumer, depends_on=[out])
    pipeline.add_task(task_a)
    pipeline.add_task(task_b)
    pipeline._solve_order()

    assert task_a in task_b.task_dependencies
    assert task_b.path_dependencies == []


# ---------------------------------------------------------------------------
# Direct Task dependency
# ---------------------------------------------------------------------------

def test_solve_order_direct_task_dependency(pipeline):
    def fn_a():
        pass

    def fn_b():
        pass

    task_a = Task("a", fn_a)
    task_b = Task("b", fn_b, depends_on=[task_a])
    pipeline.add_task(task_a)
    pipeline.add_task(task_b)
    pipeline._solve_order()

    assert task_a in task_b.task_dependencies
    assert task_b.path_dependencies == []


# ---------------------------------------------------------------------------
# Backlinks: producing task knows about its dependents
# ---------------------------------------------------------------------------

def test_solve_order_backlinks_from_path_dep(tmp_path, pipeline):
    out = tmp_path / "out.txt"

    def fn_a():
        pass

    def fn_b():
        pass

    task_a = Task("a", fn_a, produces=[out])
    task_b = Task("b", fn_b, depends_on=[out])
    pipeline.add_task(task_a)
    pipeline.add_task(task_b)
    pipeline._solve_order()

    assert task_b in task_a.dependent_tasks


def test_solve_order_backlinks_from_direct_task_dep(pipeline):
    def fn_a():
        pass

    def fn_b():
        pass

    task_a = Task("a", fn_a)
    task_b = Task("b", fn_b, depends_on=[task_a])
    pipeline.add_task(task_a)
    pipeline.add_task(task_b)
    pipeline._solve_order()

    assert task_b in task_a.dependent_tasks


# ---------------------------------------------------------------------------
# Deduplication: same upstream added via both Path and Task ref
# ---------------------------------------------------------------------------

def test_solve_order_no_duplicate_task_deps(tmp_path, pipeline):
    out = tmp_path / "out.txt"

    def fn_a():
        pass

    def fn_b():
        pass

    task_a = Task("a", fn_a, produces=[out])
    # depends on both the path AND directly on task_a → should appear once
    task_b = Task("b", fn_b, depends_on=[out, task_a])
    pipeline.add_task(task_a)
    pipeline.add_task(task_b)
    pipeline._solve_order()

    assert task_b.task_dependencies.count(task_a) == 1


# ---------------------------------------------------------------------------
# Multiple independent tasks
# ---------------------------------------------------------------------------

def test_solve_order_multiple_independent_tasks(pipeline):
    def fn1():
        pass

    def fn2():
        pass

    t1 = Task("t1", fn1)
    t2 = Task("t2", fn2)
    pipeline.add_tasks([t1, t2])
    pipeline._solve_order()

    assert t1.task_dependencies == []
    assert t2.task_dependencies == []
    assert t1.path_dependencies == []
    assert t2.path_dependencies == []


# ---------------------------------------------------------------------------
# Diamond dependency: A→B→D, A→C→D
# ---------------------------------------------------------------------------

def test_solve_order_diamond(tmp_path, pipeline):
    out_b = tmp_path / "b.txt"
    out_c = tmp_path / "c.txt"

    def fn_a():
        pass

    def fn_b():
        pass

    def fn_c():
        pass

    def fn_d():
        pass

    task_a = Task("a", fn_a)
    task_b = Task("b", fn_b, depends_on=[task_a], produces=[out_b])
    task_c = Task("c", fn_c, depends_on=[task_a], produces=[out_c])
    task_d = Task("d", fn_d, depends_on=[out_b, out_c])

    pipeline.add_task(task_a)
    pipeline.add_task(task_b)
    pipeline.add_task(task_c)
    pipeline.add_task(task_d)
    pipeline._solve_order()

    assert task_b in task_d.task_dependencies
    assert task_c in task_d.task_dependencies
    assert task_d in task_b.dependent_tasks
    assert task_d in task_c.dependent_tasks
