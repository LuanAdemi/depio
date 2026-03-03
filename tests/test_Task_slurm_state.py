"""
Tests for Task._set_status_by_slurmstate(): every SLURM state group
maps to the expected TaskStatus.
"""
import pytest
from depio.Task import Task
from depio.TaskStatus import TaskStatus


def _fresh():
    t = Task("t", lambda: None)
    # slurmjob is None, so set_to_failed won't call slurmjob.cancel()
    return t


# ---------------------------------------------------------------------------
# RUNNING group
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", ["RUNNING", "CONFIGURING", "COMPLETING", "STAGE_OUT"])
def test_running_states(state):
    task = _fresh()
    result = task._set_status_by_slurmstate(state)
    assert result == TaskStatus.RUNNING
    assert task._status == TaskStatus.RUNNING


# ---------------------------------------------------------------------------
# FAILED group
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", [
    "FAILED", "BOOT_FAIL", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY",
    "PREEMPTED", "SPECIAL_EXIT", "STOPPED", "SUSPENDED", "TIMEOUT",
])
def test_failed_states(state):
    task = _fresh()
    result = task._set_status_by_slurmstate(state)
    assert result == TaskStatus.FAILED
    assert task._status == TaskStatus.FAILED


# ---------------------------------------------------------------------------
# PENDING group
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", ["READY", "PENDING", "REQUEUE_FED", "REQUEUED"])
def test_pending_states(state):
    task = _fresh()
    result = task._set_status_by_slurmstate(state)
    assert result == TaskStatus.PENDING
    assert task._status == TaskStatus.PENDING


# ---------------------------------------------------------------------------
# CANCELLED
# ---------------------------------------------------------------------------

def test_cancelled_exact():
    task = _fresh()
    result = task._set_status_by_slurmstate("CANCELLED")
    assert result == TaskStatus.CANCELED
    assert task._status == TaskStatus.CANCELED


def test_cancelled_by_user_string():
    # Slurm sometimes returns "CANCELLED by 12345"
    task = _fresh()
    result = task._set_status_by_slurmstate("CANCELLED by 12345")
    assert result == TaskStatus.CANCELED
    assert task._status == TaskStatus.CANCELED


# ---------------------------------------------------------------------------
# COMPLETED
# ---------------------------------------------------------------------------

def test_completed_state():
    task = _fresh()
    result = task._set_status_by_slurmstate("COMPLETED")
    assert result == TaskStatus.FINISHED
    assert task._status == TaskStatus.FINISHED


# ---------------------------------------------------------------------------
# HOLD group
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", ["RESV_DEL_HOLD", "REQUEUE_HOLD", "RESIZING", "REVOKED", "SIGNALING"])
def test_hold_states(state):
    task = _fresh()
    result = task._set_status_by_slurmstate(state)
    assert result == TaskStatus.HOLD
    assert task._status == TaskStatus.HOLD


# ---------------------------------------------------------------------------
# UNKNOWN
# ---------------------------------------------------------------------------

def test_unknown_state_keyword():
    task = _fresh()
    result = task._set_status_by_slurmstate("UNKNOWN")
    assert result == TaskStatus.UNKNOWN
    assert task._status == TaskStatus.UNKNOWN


def test_unrecognised_state_falls_through_to_unknown():
    task = _fresh()
    result = task._set_status_by_slurmstate("SOME_MADE_UP_STATE")
    assert result == TaskStatus.UNKNOWN
    assert task._status == TaskStatus.UNKNOWN


# ---------------------------------------------------------------------------
# Return value matches stored status
# ---------------------------------------------------------------------------

def test_return_value_matches_stored_status():
    task = _fresh()
    returned = task._set_status_by_slurmstate("RUNNING")
    assert returned == task._status


# ---------------------------------------------------------------------------
# Failed state propagates to dependents
# ---------------------------------------------------------------------------

def test_failed_state_propagates_depfailed_to_dependents():
    parent = _fresh()
    child = _fresh()
    parent.add_dependent_task(child)

    parent._set_status_by_slurmstate("FAILED")

    assert child._status == TaskStatus.DEPFAILED
