"""
Tests for Task.is_ready_for_execution(), all_path_dependencies_exist(),
and all_task_dependencies_terminated_successfully().
"""
import pytest
from pathlib import Path

from depio.Task import Task
from depio.TaskStatus import TaskStatus
from depio.BuildMode import BuildMode


def _dummy():
    pass


def _make(buildmode=BuildMode.ALWAYS, produces=None, path_deps=None, task_deps=None):
    task = Task("t", _dummy, produces=produces or [], buildmode=buildmode)
    task.path_dependencies = path_deps if path_deps is not None else []
    task.task_dependencies = task_deps if task_deps is not None else []
    return task


# ---------------------------------------------------------------------------
# should_run() returns False → skipped, ready returns False
# ---------------------------------------------------------------------------

def test_not_ready_when_should_run_false_no_products():
    # IF_MISSING + no products → should_run() = False → SKIPPED
    task = _make(buildmode=BuildMode.IF_MISSING)
    result = task.is_ready_for_execution()
    assert result is False
    assert task._status == TaskStatus.SKIPPED


def test_not_ready_when_buildmode_never():
    task = _make(buildmode=BuildMode.NEVER)
    result = task.is_ready_for_execution()
    assert result is False
    assert task._status == TaskStatus.SKIPPED


def test_not_ready_when_product_exists_and_if_missing(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("data")
    task = _make(buildmode=BuildMode.IF_MISSING, produces=[out])
    result = task.is_ready_for_execution()
    assert result is False
    assert task._status == TaskStatus.SKIPPED


# ---------------------------------------------------------------------------
# Path dependency missing → DEPFAILED
# ---------------------------------------------------------------------------

def test_not_ready_when_path_dep_missing(tmp_path):
    missing = tmp_path / "missing.txt"
    task = _make(buildmode=BuildMode.ALWAYS, path_deps=[missing])
    result = task.is_ready_for_execution()
    assert result is False
    assert task._status == TaskStatus.DEPFAILED


# ---------------------------------------------------------------------------
# Already in terminal state
# ---------------------------------------------------------------------------

def test_not_ready_when_already_finished():
    task = _make(buildmode=BuildMode.ALWAYS)
    task._status = TaskStatus.FINISHED
    assert task.is_ready_for_execution() is False


def test_not_ready_when_already_failed():
    task = _make(buildmode=BuildMode.ALWAYS)
    task._status = TaskStatus.FAILED
    assert task.is_ready_for_execution() is False


def test_not_ready_when_already_skipped():
    task = _make(buildmode=BuildMode.ALWAYS)
    task._status = TaskStatus.SKIPPED
    assert task.is_ready_for_execution() is False


# ---------------------------------------------------------------------------
# Task dependency not finished
# ---------------------------------------------------------------------------

def test_not_ready_when_task_dep_still_running():
    dep = Task("dep", _dummy)
    dep._status = TaskStatus.RUNNING
    dep.path_dependencies = []
    dep.task_dependencies = []

    task = _make(buildmode=BuildMode.ALWAYS, task_deps=[dep])
    assert task.is_ready_for_execution() is False


def test_not_ready_when_task_dep_waiting():
    dep = Task("dep", _dummy)
    dep._status = TaskStatus.WAITING  # not a successful terminal state
    dep.path_dependencies = []
    dep.task_dependencies = []

    task = _make(buildmode=BuildMode.ALWAYS, task_deps=[dep])
    assert task.is_ready_for_execution() is False


# ---------------------------------------------------------------------------
# Ready to run
# ---------------------------------------------------------------------------

def test_ready_with_no_deps():
    task = _make(buildmode=BuildMode.ALWAYS)
    assert task.is_ready_for_execution() is True


def test_ready_with_existing_path_dep(tmp_path):
    dep = tmp_path / "dep.txt"
    dep.write_text("here")
    task = _make(buildmode=BuildMode.ALWAYS, path_deps=[dep])
    assert task.is_ready_for_execution() is True


def test_ready_when_task_dep_finished():
    dep = Task("dep", _dummy)
    dep._status = TaskStatus.FINISHED
    dep.path_dependencies = []
    dep.task_dependencies = []

    task = _make(buildmode=BuildMode.ALWAYS, task_deps=[dep])
    assert task.is_ready_for_execution() is True


def test_ready_when_task_dep_skipped():
    dep = Task("dep", _dummy)
    dep._status = TaskStatus.SKIPPED
    dep.path_dependencies = []
    dep.task_dependencies = []

    task = _make(buildmode=BuildMode.ALWAYS, task_deps=[dep])
    assert task.is_ready_for_execution() is True


# ---------------------------------------------------------------------------
# all_path_dependencies_exist
# ---------------------------------------------------------------------------

def test_all_path_deps_exist_when_empty():
    task = _make()
    assert task.all_path_dependencies_exist() is True


def test_all_path_deps_exist_when_file_present(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text("data")
    task = _make(path_deps=[f])
    assert task.all_path_dependencies_exist() is True


def test_all_path_deps_exist_false_when_missing(tmp_path):
    task = _make(path_deps=[tmp_path / "no.txt"])
    assert task.all_path_dependencies_exist() is False


# ---------------------------------------------------------------------------
# all_task_dependencies_terminated_successfully
# ---------------------------------------------------------------------------

def test_all_task_deps_done_when_empty():
    task = _make()
    assert task.all_task_dependencies_terminated_successfully() is True


def test_all_task_deps_done_when_dep_finished():
    dep = Task("dep", _dummy)
    dep._status = TaskStatus.FINISHED
    task = _make(task_deps=[dep])
    assert task.all_task_dependencies_terminated_successfully() is True


def test_all_task_deps_not_done_when_dep_running():
    dep = Task("dep", _dummy)
    dep._status = TaskStatus.RUNNING
    task = _make(task_deps=[dep])
    assert task.all_task_dependencies_terminated_successfully() is False
