"""
Tests for Task.should_run() across all BuildMode values.
"""
import pytest
from pathlib import Path

import depio.code_hash as _ch_mod
from depio.Task import Task
from depio.BuildMode import BuildMode


# Redirect hash file to tmp so tests never touch .depio/task_hashes.json
@pytest.fixture(autouse=True)
def isolated_hash_file(tmp_path, monkeypatch):
    monkeypatch.setattr(_ch_mod, "_HASH_FILE", tmp_path / "task_hashes.json")


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


# ---------------------------------------------------------------------------
# BuildMode.IF_CODE_CHANGED
# ---------------------------------------------------------------------------

def test_if_code_changed_missing_product_runs(tmp_path):
    out = tmp_path / "missing.txt"
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_CODE_CHANGED)
    task.task_dependencies = []
    assert task.should_run() is True


def test_if_code_changed_unseen_hash_runs(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("exists")
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_CODE_CHANGED)
    task.task_dependencies = []
    # No hash recorded yet → treated as changed
    assert task.should_run() is True


def test_if_code_changed_same_hash_skips(tmp_path):
    from depio.code_hash import record_hash
    out = tmp_path / "out.txt"
    out.write_text("exists")
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_CODE_CHANGED)
    task.task_dependencies = []
    record_hash(task._code_hash_key, _dummy)
    assert task.should_run() is False


def test_if_code_changed_detects_new_function(tmp_path):
    from depio.code_hash import record_hash
    out = tmp_path / "out.txt"
    out.write_text("exists")

    def _v1():
        return 1

    def _v2():
        return 2

    task = Task("t", _v1, produces=[out], buildmode=BuildMode.IF_CODE_CHANGED)
    task.task_dependencies = []
    # Record hash for _v2 under the same key → simulates function body change
    record_hash(task._code_hash_key, _v2)
    assert task.should_run() is True


# ---------------------------------------------------------------------------
# track_code flag (works on top of any buildmode)
# ---------------------------------------------------------------------------

def test_track_code_triggers_on_unseen_hash(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("exists")
    # IF_MISSING would normally skip (product exists), but track_code adds code check
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING, track_code=True)
    task.task_dependencies = []
    assert task.should_run() is True


def test_track_code_skips_when_hash_matches(tmp_path):
    from depio.code_hash import record_hash
    out = tmp_path / "out.txt"
    out.write_text("exists")
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING, track_code=True)
    task.task_dependencies = []
    record_hash(task._code_hash_key, _dummy)
    assert task.should_run() is False


def test_track_code_does_not_suppress_buildmode_trigger(tmp_path):
    from depio.code_hash import record_hash
    out = tmp_path / "missing.txt"            # product absent → IF_MISSING triggers
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING, track_code=True)
    task.task_dependencies = []
    record_hash(task._code_hash_key, _dummy)  # hash matches — code hasn't changed
    # Should still run because the product is missing
    assert task.should_run() is True


def test_track_code_false_does_not_add_code_check(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("exists")
    # track_code=False (default) — no hash recorded, but IF_MISSING won't trigger either
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING, track_code=False)
    task.task_dependencies = []
    assert task.should_run() is False


# ---------------------------------------------------------------------------
# Universal upstream propagation invariant
#
# If task A decides to run (should_run() → True), every downstream task B
# must also run, regardless of B's own build mode.
# Tasks are evaluated in topological order (as Pipeline does), so A's
# _decided_to_run flag is set before B's should_run() is called.
# ---------------------------------------------------------------------------

def _make_upstream(buildmode=BuildMode.ALWAYS):
    """Return a task that will decide to run (no products, deps already evaluated)."""
    t = Task("upstream", _dummy, buildmode=buildmode)
    t.task_dependencies = []
    t.should_run()   # sets _decided_to_run = True for ALWAYS
    return t


def test_upstream_ran_propagates_to_if_missing(tmp_path):
    out = tmp_path / "out.txt"
    out.write_text("exists")          # IF_MISSING would normally skip
    upstream = _make_upstream()

    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING)
    task.task_dependencies = [upstream]

    assert task.should_run() is True


def test_upstream_ran_propagates_to_never(tmp_path):
    upstream = _make_upstream()

    task = Task("t", _dummy, buildmode=BuildMode.NEVER)
    task.task_dependencies = [upstream]

    assert task.should_run() is True


def test_upstream_ran_propagates_to_if_code_changed(tmp_path):
    from depio.code_hash import record_hash
    out = tmp_path / "out.txt"
    out.write_text("exists")
    upstream = _make_upstream()

    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_CODE_CHANGED)
    task.task_dependencies = [upstream]
    record_hash(task._code_hash_key, _dummy)   # hash matches — code unchanged

    assert task.should_run() is True


def test_upstream_not_running_does_not_propagate(tmp_path):
    # Upstream uses NEVER → decides not to run → _decided_to_run stays False
    upstream = Task("upstream", _dummy, buildmode=BuildMode.NEVER)
    upstream.task_dependencies = []
    upstream.should_run()   # → False

    out = tmp_path / "out.txt"
    out.write_text("exists")
    task = Task("t", _dummy, produces=[out], buildmode=BuildMode.IF_MISSING)
    task.task_dependencies = [upstream]

    assert task.should_run() is False


def test_propagation_chains_through_intermediate_task(tmp_path):
    # A (ALWAYS) → B (IF_MISSING, product exists) → C (IF_MISSING, product exists)
    # A runs → B's flag set → C runs
    a = Task("a", _dummy, buildmode=BuildMode.ALWAYS)
    a.task_dependencies = []
    a.should_run()   # → True, _decided_to_run = True

    b_out = tmp_path / "b.txt"
    b_out.write_text("exists")
    b = Task("b", _dummy, produces=[b_out], buildmode=BuildMode.IF_MISSING)
    b.task_dependencies = [a]
    b.should_run()   # propagated from a → True, _decided_to_run = True

    c_out = tmp_path / "c.txt"
    c_out.write_text("exists")
    c = Task("c", _dummy, produces=[c_out], buildmode=BuildMode.IF_MISSING)
    c.task_dependencies = [b]

    assert c.should_run() is True


def test_propagation_does_not_chain_when_upstream_skips(tmp_path):
    # A (NEVER) → B (IF_MISSING, product exists) → C (IF_MISSING, product exists)
    # A skips → B skips → C skips
    a = Task("a", _dummy, buildmode=BuildMode.NEVER)
    a.task_dependencies = []
    a.should_run()   # → False

    b_out = tmp_path / "b.txt"
    b_out.write_text("exists")
    b = Task("b", _dummy, produces=[b_out], buildmode=BuildMode.IF_MISSING)
    b.task_dependencies = [a]
    b.should_run()   # → False

    c_out = tmp_path / "c.txt"
    c_out.write_text("exists")
    c = Task("c", _dummy, produces=[c_out], buildmode=BuildMode.IF_MISSING)
    c.task_dependencies = [b]

    assert c.should_run() is False
