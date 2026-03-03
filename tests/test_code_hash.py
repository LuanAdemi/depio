"""
Tests for src/depio/code_hash.py

Covers: compute_hash, has_code_changed, record_hash, and the _load/_save
persistence layer.  Each test runs with an isolated hash file via the
`isolated_hash_file` fixture so the real .depio/task_hashes.json is never
touched.
"""
import json
import pytest

import depio.code_hash as _mod
from depio.code_hash import compute_hash, has_code_changed, record_hash


# ---------------------------------------------------------------------------
# Fixture — redirect _HASH_FILE to a tmp directory for every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_hash_file(tmp_path, monkeypatch):
    monkeypatch.setattr(_mod, "_HASH_FILE", tmp_path / "task_hashes.json")


# ---------------------------------------------------------------------------
# Helper functions used as stable "source" for hashing
# ---------------------------------------------------------------------------

def _func_a():
    return 1


def _func_b():
    return 2


# ---------------------------------------------------------------------------
# compute_hash
# ---------------------------------------------------------------------------

def test_compute_hash_returns_nonempty_string():
    h = compute_hash(_func_a)
    assert isinstance(h, str) and len(h) == 64  # SHA-256 hex digest


def test_compute_hash_is_deterministic():
    assert compute_hash(_func_a) == compute_hash(_func_a)


def test_compute_hash_differs_between_functions():
    assert compute_hash(_func_a) != compute_hash(_func_b)


def test_compute_hash_uninspectable_returns_empty_string(monkeypatch):
    import inspect
    monkeypatch.setattr(inspect, "getsource", lambda _: (_ for _ in ()).throw(OSError()))
    assert compute_hash(_func_a) == ""


# ---------------------------------------------------------------------------
# has_code_changed — unseen key
# ---------------------------------------------------------------------------

def test_has_code_changed_unseen_key_returns_true():
    assert has_code_changed("brand_new_key", _func_a) is True


# ---------------------------------------------------------------------------
# has_code_changed — after record_hash
# ---------------------------------------------------------------------------

def test_has_code_changed_false_after_record():
    record_hash("k", _func_a)
    assert has_code_changed("k", _func_a) is False


def test_has_code_changed_true_when_different_func_recorded():
    record_hash("k", _func_a)
    assert has_code_changed("k", _func_b) is True


def test_has_code_changed_false_for_uninspectable_function(monkeypatch):
    """Uninspectable functions should never be treated as changed."""
    import inspect
    monkeypatch.setattr(inspect, "getsource", lambda _: (_ for _ in ()).throw(OSError()))
    assert has_code_changed("k", _func_a) is False


# ---------------------------------------------------------------------------
# record_hash — persistence
# ---------------------------------------------------------------------------

def test_record_hash_writes_json(tmp_path, monkeypatch):
    hash_file = tmp_path / "task_hashes.json"
    monkeypatch.setattr(_mod, "_HASH_FILE", hash_file)
    record_hash("mykey", _func_a)
    data = json.loads(hash_file.read_text())
    assert "mykey" in data
    assert data["mykey"] == compute_hash(_func_a)


def test_record_hash_creates_parent_dir(tmp_path, monkeypatch):
    nested = tmp_path / "subdir" / "hashes.json"
    monkeypatch.setattr(_mod, "_HASH_FILE", nested)
    record_hash("k", _func_a)
    assert nested.exists()


def test_record_hash_updates_existing_entry(tmp_path, monkeypatch):
    hash_file = tmp_path / "task_hashes.json"
    monkeypatch.setattr(_mod, "_HASH_FILE", hash_file)
    record_hash("k", _func_a)
    record_hash("k", _func_b)
    data = json.loads(hash_file.read_text())
    assert data["k"] == compute_hash(_func_b)


def test_record_hash_preserves_other_keys(tmp_path, monkeypatch):
    hash_file = tmp_path / "task_hashes.json"
    monkeypatch.setattr(_mod, "_HASH_FILE", hash_file)
    record_hash("key1", _func_a)
    record_hash("key2", _func_b)
    data = json.loads(hash_file.read_text())
    assert "key1" in data and "key2" in data


def test_record_hash_noop_for_uninspectable_function(tmp_path, monkeypatch):
    hash_file = tmp_path / "task_hashes.json"
    monkeypatch.setattr(_mod, "_HASH_FILE", hash_file)
    import inspect
    monkeypatch.setattr(inspect, "getsource", lambda _: (_ for _ in ()).throw(OSError()))
    record_hash("k", _func_a)
    assert not hash_file.exists()


# ---------------------------------------------------------------------------
# Multiple keys coexist
# ---------------------------------------------------------------------------

def test_multiple_keys_independent():
    record_hash("a", _func_a)
    record_hash("b", _func_b)
    assert has_code_changed("a", _func_a) is False
    assert has_code_changed("b", _func_b) is False
    assert has_code_changed("a", _func_b) is True
    assert has_code_changed("b", _func_a) is True
