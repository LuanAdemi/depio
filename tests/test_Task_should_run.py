"""
Tests for Task.should_run() across all four BuildMode values.
"""
import pytest
from pathlib import Path

from depio.Task import Task
from depio.BuildMode import BuildMode


def _dummy():
    pass


# ---------------------------------------------------------------------------
# BuildMode.ALWAYS
# ---------------------------------------------------------------------------

def test_always_with_no_products():
    task = Task("t", _dummy, buildmode=BuildMode.ALWAYS)
    task.task_dependencies = []
    assert task.should_run() is True


def test_always_with_existing_product(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("exists")
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.ALWAYS)
    task.task_dependencies = []
    assert task.should_run() is True


def test_always_with_missing_product(tmp_path):
    out = tmp_path / "missing.txt"
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.ALWAYS)
    task.task_dependencies = []
    assert task.should_run() is True


# ---------------------------------------------------------------------------
# BuildMode.NEVER
# ---------------------------------------------------------------------------

def test_never_with_no_products():
    task = Task("t", _dummy, buildmode=BuildMode.NEVER)
    task.task_dependencies = []
    assert task.should_run() is False


def test_never_with_missing_product(tmp_path):
    out = tmp_path / "missing.txt"
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.NEVER)
    task.task_dependencies = []
    assert task.should_run() is False


# ---------------------------------------------------------------------------
# BuildMode.IF_MISSING
# ---------------------------------------------------------------------------

def test_if_missing_with_no_products():
    # No products means nothing is missing → should NOT run
    task = Task("t", _dummy, buildmode=BuildMode.IF_MISSING)
    task.task_dependencies = []
    assert task.should_run() is False


def test_if_missing_all_products_exist(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("data")
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING)
    task.task_dependencies = []
    assert task.should_run() is False


def test_if_missing_product_absent(tmp_path):
    out = tmp_path / "missing.txt"
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING)
    task.task_dependencies = []
    assert task.should_run() is True


def test_if_missing_one_of_two_absent(tmp_path):
    existing = tmp_path / "existing.txt"
    existing.write_text("here")
    missing = tmp_path / "missing.txt"
    task = Task("t", _dummy, produces=[existing, missing], buildmode=BuildMode.IF_MISSING)
    task.task_dependencies = []
    assert task.should_run() is True


# ---------------------------------------------------------------------------
# BuildMode.IF_NEW
# ---------------------------------------------------------------------------

def test_if_new_no_products_no_deps():
    task = Task("t", _dummy, buildmode=BuildMode.IF_NEW)
    task.task_dependencies = []
    # No missing products, no deps that should run → False
    assert task.should_run() is False


def test_if_new_missing_product(tmp_path):
    out = tmp_path / "missing.txt"
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_NEW)
    task.task_dependencies = []
    assert task.should_run() is True


def test_if_new_dep_should_run(tmp_path):
    dep_out = tmp_path / "dep.txt"           # missing → dep_task.should_run() True
    own_out = tmp_path / "own.txt"
    own_out.write_text("exists")             # own product exists

    dep_task = Task("dep", _dummy, produces=[dep_out], buildmode=BuildMode.IF_MISSING)
    dep_task.task_dependencies = []

    task = Task("t", _dummy, produces=[own_out], buildmode=BuildMode.IF_NEW)
    task.task_dependencies = [dep_task]

    assert task.should_run() is True


def test_if_new_dep_does_not_need_to_run(tmp_path):
    dep_out = tmp_path / "dep.txt"
    dep_out.write_text("exists")             # dep product exists → dep won't run
    own_out = tmp_path / "own.txt"
    own_out.write_text("exists")             # own product exists

    dep_task = Task("dep", _dummy, produces=[dep_out], buildmode=BuildMode.IF_MISSING)
    dep_task.task_dependencies = []

    task = Task("t", _dummy, produces=[own_out], buildmode=BuildMode.IF_NEW)
    task.task_dependencies = [dep_task]

    assert task.should_run() is False
