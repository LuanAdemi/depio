"""
Tests for Task.run() and Task.barerun(): success paths, failure paths,
timing, and dependency/product checks.
"""
import time
import pytest
from pathlib import Path

from depio.Task import Task, Product
from depio.TaskStatus import TaskStatus
from depio.exceptions import (
    DependencyNotMetException,
    ProductNotProducedException,
    ProductNotUpdatedException,
    TaskRaisedException,
)
from depio.BuildMode import BuildMode


def _make_runnable(task):
    """Attach empty path/task dependency lists so task.run() can proceed."""
    task.path_dependencies = []
    task.task_dependencies = []
    return task


# ---------------------------------------------------------------------------
# Success paths
# ---------------------------------------------------------------------------

def test_run_no_products_finishes():
    ran = []

    def func():
        ran.append(True)

    task = _make_runnable(Task("t", func, buildmode=BuildMode.ALWAYS))
    task.run()

    assert ran == [True]
    assert task._status == TaskStatus.FINISHED


def test_run_creates_product(tmp_path):
    out = tmp_path / "out.txt"

    def func():
        out.write_text("hello")

    task = _make_runnable(Task("t", func, produces=[out]))
    task.run()

    assert task._status == TaskStatus.FINISHED
    assert out.read_text() == "hello"


def test_run_sets_status_to_running_during_execution():
    statuses = []

    def func():
        statuses.append(task._status)

    task = _make_runnable(Task("t", func, buildmode=BuildMode.ALWAYS))
    task.run()

    assert TaskStatus.RUNNING in statuses


def test_run_sets_start_and_end_time():
    def func():
        pass

    task = _make_runnable(Task("t", func, buildmode=BuildMode.ALWAYS))
    before = time.time()
    task.run()
    after = time.time()

    assert task.start_time is not None
    assert task.end_time is not None
    assert before <= task.start_time <= after
    assert task.end_time >= task.start_time


def test_run_start_time_set_before_end_time():
    def func():
        pass

    task = _make_runnable(Task("t", func, buildmode=BuildMode.ALWAYS))
    task.run()

    assert task.start_time <= task.end_time


# ---------------------------------------------------------------------------
# Missing path dependency
# ---------------------------------------------------------------------------

def test_run_raises_when_path_dep_missing(tmp_path):
    missing = tmp_path / "does_not_exist.txt"

    task = Task("t", lambda: None, buildmode=BuildMode.ALWAYS)
    task.path_dependencies = [missing]
    task.task_dependencies = []

    with pytest.raises(DependencyNotMetException):
        task.run()

    assert task._status == TaskStatus.FAILED


# ---------------------------------------------------------------------------
# Function raises an exception
# ---------------------------------------------------------------------------

def test_run_wraps_exception_in_task_raised_exception():
    def boom():
        raise ValueError("test error")

    task = _make_runnable(Task("t", boom, buildmode=BuildMode.ALWAYS))

    with pytest.raises(TaskRaisedException):
        task.run()

    assert task._status == TaskStatus.FAILED


def test_run_exception_does_not_set_end_time():
    def boom():
        raise RuntimeError("oops")

    task = _make_runnable(Task("t", boom, buildmode=BuildMode.ALWAYS))

    with pytest.raises(TaskRaisedException):
        task.run()

    assert task.end_time is None


# ---------------------------------------------------------------------------
# Product not produced
# ---------------------------------------------------------------------------

def test_run_raises_when_product_not_created(tmp_path):
    out = tmp_path / "never_created.txt"

    def func():
        pass  # deliberately skips creating the output

    task = _make_runnable(Task("t", func, produces=[out]))

    with pytest.raises(ProductNotProducedException):
        task.run()

    assert task._status == TaskStatus.FAILED


# ---------------------------------------------------------------------------
# Product not updated
# ---------------------------------------------------------------------------

def test_run_raises_when_product_not_updated(tmp_path):
    out = tmp_path / "stale.txt"
    out.write_text("initial content")  # product already exists

    def func():
        pass  # deliberately does NOT touch the file

    task = _make_runnable(Task("t", func, produces=[out]))

    with pytest.raises(ProductNotUpdatedException):
        task.run()


# ---------------------------------------------------------------------------
# barerun
# ---------------------------------------------------------------------------

def test_barerun_calls_func():
    ran = []

    def func():
        ran.append(True)

    task = Task("t", func)
    task.barerun()

    assert ran == [True]


def test_barerun_passes_args():
    received = []

    def func(a, b):
        received.append((a, b))

    task = Task("t", func, func_args=[1, 2])
    task.barerun()

    assert received == [(1, 2)]


def test_barerun_passes_kwargs():
    received = {}

    def func(a, b=0):
        received["a"] = a
        received["b"] = b

    task = Task("t", func, func_args=[10], func_kwargs={"b": 20})
    task.barerun()

    assert received == {"a": 10, "b": 20}
