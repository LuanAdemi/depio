"""
Tests for Task status transition methods, is_in_* properties, and
DEPFAILED propagation through the dependent task chain.
"""
import pytest
from depio.Task import Task
from depio.TaskStatus import (
    TaskStatus,
    TERMINAL_STATES,
    SUCCESSFUL_TERMINAL_STATES,
    FAILED_TERMINAL_STATES,
)


def _dummy():
    pass


def _fresh():
    return Task("t", _dummy)


# ---------------------------------------------------------------------------
# is_in_terminal_state
# ---------------------------------------------------------------------------

def test_is_in_terminal_state_for_all_statuses():
    task = _fresh()
    for status in TaskStatus:
        task._status = status
        expected = status in TERMINAL_STATES
        assert task.is_in_terminal_state == expected, f"Failed for {status}"


# ---------------------------------------------------------------------------
# is_in_successful_terminal_state
# ---------------------------------------------------------------------------

def test_is_in_successful_terminal_state_for_all_statuses():
    task = _fresh()
    for status in TaskStatus:
        task._status = status
        expected = status in SUCCESSFUL_TERMINAL_STATES
        assert task.is_in_successful_terminal_state == expected, f"Failed for {status}"


# ---------------------------------------------------------------------------
# is_in_failed_terminal_state
# ---------------------------------------------------------------------------

def test_is_in_failed_terminal_state_for_all_statuses():
    task = _fresh()
    for status in TaskStatus:
        task._status = status
        expected = status in FAILED_TERMINAL_STATES
        assert task.is_in_failed_terminal_state == expected, f"Failed for {status}"


# ---------------------------------------------------------------------------
# set_to_failed
# ---------------------------------------------------------------------------

def test_set_to_failed_sets_status():
    task = _fresh()
    task.set_to_failed()
    assert task._status == TaskStatus.FAILED


def test_set_to_failed_propagates_depfailed_to_direct_dependent():
    parent = _fresh()
    child = _fresh()
    parent.add_dependent_task(child)

    parent.set_to_failed()

    assert child._status == TaskStatus.DEPFAILED


def test_set_to_failed_propagates_through_chain():
    a = _fresh()
    b = _fresh()
    c = _fresh()
    a.add_dependent_task(b)
    b.add_dependent_task(c)

    a.set_to_failed()

    assert b._status == TaskStatus.DEPFAILED
    assert c._status == TaskStatus.DEPFAILED


def test_set_to_failed_does_not_affect_unrelated_task():
    a = _fresh()
    b = _fresh()  # no dependency link

    a.set_to_failed()

    assert b._status == TaskStatus.WAITING


# ---------------------------------------------------------------------------
# set_to_depfailed
# ---------------------------------------------------------------------------

def test_set_to_depfailed_sets_status():
    task = _fresh()
    task.set_to_depfailed()
    assert task._status == TaskStatus.DEPFAILED


def test_set_to_depfailed_propagates_to_dependents():
    parent = _fresh()
    child = _fresh()
    parent.add_dependent_task(child)

    parent.set_to_depfailed()

    assert child._status == TaskStatus.DEPFAILED


# ---------------------------------------------------------------------------
# set_to_skipped
# ---------------------------------------------------------------------------

def test_set_to_skipped_sets_status():
    task = _fresh()
    task.set_to_skipped()
    assert task._status == TaskStatus.SKIPPED


def test_set_to_skipped_does_not_propagate():
    parent = _fresh()
    child = _fresh()
    parent.add_dependent_task(child)

    parent.set_to_skipped()

    assert child._status == TaskStatus.WAITING  # unaffected


# ---------------------------------------------------------------------------
# set_dependent_task_to_depfailed
# ---------------------------------------------------------------------------

def test_set_dependent_task_to_depfailed_with_no_dependents():
    task = _fresh()
    task.set_dependent_task_to_depfailed()  # should not raise


def test_set_dependent_task_to_depfailed_multiple_dependents():
    parent = _fresh()
    child1 = _fresh()
    child2 = _fresh()
    parent.add_dependent_task(child1)
    parent.add_dependent_task(child2)

    parent.set_dependent_task_to_depfailed()

    assert child1._status == TaskStatus.DEPFAILED
    assert child2._status == TaskStatus.DEPFAILED


# ---------------------------------------------------------------------------
# add_dependent_task
# ---------------------------------------------------------------------------

def test_add_dependent_task_appends():
    a = _fresh()
    b = _fresh()
    c = _fresh()
    a.add_dependent_task(b)
    a.add_dependent_task(c)
    assert b in a.dependent_tasks
    assert c in a.dependent_tasks
    assert len(a.dependent_tasks) == 2
