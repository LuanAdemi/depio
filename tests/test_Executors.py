"""
Tests for AbstractTaskExecutor properties, SequentialExecutor, and
ParallelExecutor. SubmitItExecutor requires a real SLURM cluster and
is excluded from unit tests.
"""
import pytest
from pathlib import Path

from depio.Executors import SequentialExecutor, ParallelExecutor
from depio.Task import Task
from depio.TaskStatus import TaskStatus
from depio.BuildMode import BuildMode


def _make_runnable_task(func=None, produces=None):
    """Return a Task with empty path/task_dependency lists ready for execution."""
    if func is None:
        def func():
            pass
    task = Task("t", func, produces=produces or [], buildmode=BuildMode.ALWAYS)
    task.path_dependencies = []
    task.task_dependencies = []
    return task


# ---------------------------------------------------------------------------
# AbstractTaskExecutor property tests (via SequentialExecutor)
# ---------------------------------------------------------------------------

def test_has_jobs_queued_limit_none():
    ex = SequentialExecutor(max_jobs_queued=None)
    assert ex.has_jobs_queued_limit is False


def test_has_jobs_queued_limit_set():
    ex = SequentialExecutor(max_jobs_queued=10)
    assert ex.has_jobs_queued_limit is True


def test_has_jobs_pending_limit_none():
    ex = SequentialExecutor(max_jobs_pending=None)
    assert ex.has_jobs_pending_limit is False


def test_has_jobs_pending_limit_set():
    ex = SequentialExecutor(max_jobs_pending=5)
    assert ex.has_jobs_pending_limit is True


# ---------------------------------------------------------------------------
# SequentialExecutor
# ---------------------------------------------------------------------------

def test_sequential_handles_dependencies_false():
    ex = SequentialExecutor()
    assert ex.handles_dependencies() is False


def test_sequential_submit_runs_task():
    ran = []

    def func():
        ran.append(True)

    task = _make_runnable_task(func)
    ex = SequentialExecutor()
    ex.submit(task, [])

    assert ran == [True]
    assert task._status == TaskStatus.FINISHED


def test_sequential_submit_creates_product(tmp_path):
    out = tmp_path / "out.txt"

    def func():
        out.write_text("done")

    task = _make_runnable_task(func, produces=[out])
    ex = SequentialExecutor()
    ex.submit(task, [])

    assert out.exists()


def test_sequential_wait_for_all_is_noop():
    ex = SequentialExecutor()
    ex.wait_for_all()  # should not raise


def test_sequential_cancel_all_jobs_is_noop():
    ex = SequentialExecutor()
    ex.cancel_all_jobs()  # should not raise


# ---------------------------------------------------------------------------
# ParallelExecutor
# ---------------------------------------------------------------------------

def test_parallel_handles_dependencies_false():
    ex = ParallelExecutor()
    assert ex.handles_dependencies() is False


def test_parallel_submit_and_wait_runs_task(tmp_path):
    out = tmp_path / "out.txt"

    def func():
        out.write_text("parallel")

    task = _make_runnable_task(func, produces=[out])
    ex = ParallelExecutor()
    ex.submit(task, [])
    ex.wait_for_all()

    assert out.exists()
    assert out.read_text() == "parallel"


def test_parallel_submit_multiple_tasks(tmp_path):
    results = []

    def make_func(i):
        def func():
            f = tmp_path / f"out{i}.txt"
            f.write_text(str(i))
            results.append(i)
        return func

    ex = ParallelExecutor()
    tasks = []
    for i in range(3):
        t = _make_runnable_task(make_func(i), produces=[tmp_path / f"out{i}.txt"])
        ex.submit(t, [])
        tasks.append(t)

    ex.wait_for_all()

    assert len(results) == 3
    for i in range(3):
        assert (tmp_path / f"out{i}.txt").exists()


def test_parallel_cancel_all_jobs_is_noop():
    ex = ParallelExecutor()
    ex.cancel_all_jobs()  # should not raise


def test_parallel_wait_for_all_with_no_jobs():
    ex = ParallelExecutor()
    ex.wait_for_all()  # no jobs submitted → should not raise
