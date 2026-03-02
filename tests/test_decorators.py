"""
Tests for the @task decorator: task creation, pipeline registration,
buildmode forwarding, and functional call semantics.
"""
import pytest

from depio.decorators import task
from depio.Task import Task
from depio.Pipeline import Pipeline
from depio.BuildMode import BuildMode


@pytest.fixture
def pipeline():
    return Pipeline(None, quiet=True)


# ---------------------------------------------------------------------------
# Basic task creation
# ---------------------------------------------------------------------------

def test_decorator_returns_task_instance():
    @task("my_task")
    def fn():
        pass

    result = fn()
    assert isinstance(result, Task)


def test_decorator_sets_task_name():
    @task("named_task")
    def fn():
        pass

    result = fn()
    assert result.name == "named_task"


def test_decorator_does_not_call_function_at_decoration_time():
    called = []

    @task("t")
    def fn():
        called.append(True)

    assert called == []  # function not yet called
    fn()
    assert called == []  # decorator returns Task, doesn't execute fn


# ---------------------------------------------------------------------------
# Pipeline registration
# ---------------------------------------------------------------------------

def test_decorator_with_pipeline_adds_task(pipeline):
    @task("t", pipeline=pipeline)
    def fn():
        pass

    result = fn()
    assert result in pipeline.tasks


def test_decorator_without_pipeline_does_not_add_anywhere(pipeline):
    @task("t")
    def fn():
        pass

    result = fn()
    assert result not in pipeline.tasks


def test_decorator_returns_added_task(pipeline):
    @task("t", pipeline=pipeline)
    def fn():
        pass

    result = fn()
    assert result in pipeline.tasks
    assert isinstance(result, Task)


# ---------------------------------------------------------------------------
# BuildMode forwarding
# ---------------------------------------------------------------------------

def test_decorator_default_buildmode():
    @task("t")
    def fn():
        pass

    result = fn()
    assert result.buildmode == BuildMode.IF_MISSING


def test_decorator_always_buildmode():
    @task("t", buildmode=BuildMode.ALWAYS)
    def fn():
        pass

    result = fn()
    assert result.buildmode == BuildMode.ALWAYS


def test_decorator_never_buildmode():
    @task("t", buildmode=BuildMode.NEVER)
    def fn():
        pass

    result = fn()
    assert result.buildmode == BuildMode.NEVER


# ---------------------------------------------------------------------------
# Arguments are forwarded to Task
# ---------------------------------------------------------------------------

def test_decorator_forwards_positional_args():
    @task("t")
    def fn(a, b):
        pass

    result = fn(1, 2)
    assert result.func_args == (1, 2)


def test_decorator_forwards_kwargs():
    @task("t")
    def fn(a, b=0):
        pass

    result = fn(1, b=42)
    assert result.func_kwargs == {"b": 42}


# ---------------------------------------------------------------------------
# Calling the decorated function multiple times creates independent Tasks
# ---------------------------------------------------------------------------

def test_multiple_calls_create_independent_tasks():
    @task("t")
    def fn(x):
        pass

    t1 = fn(1)
    t2 = fn(2)

    assert t1 is not t2
    assert t1 != t2
