"""
tests/test_slurm_local.py — local testbed for SLURM integration
================================================================

Tests the SubmitItExecutor and SLURM-specific Task behaviour using a mock
that replaces submitit.AutoExecutor.  No cluster access required.

Mock design
-----------
MockJob      — simulates a submitit Job.
                 job.state is set directly by the test to simulate
                 the cluster advancing, matching real behaviour where
                 the state changes externally.
               watcher.update() is a no-op (in production it fetches
                 from the cluster; _update_by_slurmjob calls it before
                 reading job.state).
MockExecutor — simulates submitit.AutoExecutor.
                 update_parameters() stores kwargs for inspection.
                 submit() returns a fresh MockJob.
"""

import pytest
from pathlib import Path

from depio.BuildMode import BuildMode
from depio.Executors import SubmitItExecutor
from depio.Task import Task
from depio.TaskStatus import TaskStatus


# ── Mock infrastructure ────────────────────────────────────────────────────────

class MockWatcher:
    """No-op watcher — in production this fetches state from the cluster."""
    def update(self):
        pass


class MockJob:
    """Simulates a submitit Job.

    Set job.state directly in tests to simulate the cluster advancing
    through PENDING → RUNNING → COMPLETED / FAILED / CANCELLED.
    """
    def __init__(self, job_id: str, state: str = "PENDING"):
        self.job_id  = job_id
        self.task_id = "0"
        self.state   = state
        self.watcher = MockWatcher()

    def cancel(self):
        self.state = "CANCELLED"


class MockExecutor:
    """Simulates submitit.AutoExecutor without touching a real cluster."""

    def __init__(self, initial_state: str = "PENDING"):
        self._counter       = 0
        self.submitted: list[tuple] = []
        self.params_history: list[dict] = []
        self._initial_state = initial_state

    def update_parameters(self, **kwargs):
        self.params_history.append(dict(kwargs))

    def submit(self, func) -> MockJob:
        self._counter += 1
        job = MockJob(str(self._counter), self._initial_state)
        self.submitted.append((func, job))
        return job

    @property
    def last_params(self) -> dict:
        return self.params_history[-1] if self.params_history else {}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _simple_task(name: str = "t") -> Task:
    def func(): pass
    t = Task(name, func, buildmode=BuildMode.IF_MISSING)
    t.path_dependencies = []
    t.task_dependencies = []
    return t


def _make_executor(initial_state="PENDING") -> tuple[MockExecutor, SubmitItExecutor]:
    mock  = MockExecutor(initial_state)
    slurm = SubmitItExecutor(internal_executor=mock)
    return mock, slurm


# ── SubmitItExecutor.submit ────────────────────────────────────────────────────

def test_submit_assigns_slurmjob():
    mock, slurm = _make_executor()
    t = _simple_task()
    slurm.submit(t, [])
    assert t.slurmjob is not None
    assert t.slurmjob.job_id == "1"


def test_submit_no_dependency_sets_no_afterok():
    mock, slurm = _make_executor()
    t = _simple_task()
    slurm.submit(t, [])
    additional = mock.last_params.get("slurm_additional_parameters", {})
    assert "dependency" not in additional


def test_submit_with_upstream_sets_afterok():
    mock, slurm = _make_executor()
    t_a = _simple_task("a")
    t_b = _simple_task("b")

    slurm.submit(t_a, [])
    slurm.submit(t_b, [t_a])

    additional = mock.last_params.get("slurm_additional_parameters", {})
    assert additional.get("dependency") == "afterok:1"


def test_submit_two_upstreams_sets_colon_separated_afterok():
    mock, slurm = _make_executor()
    t_a = _simple_task("a")
    t_b = _simple_task("b")
    t_c = _simple_task("c")

    slurm.submit(t_a, [])
    slurm.submit(t_b, [])
    slurm.submit(t_c, [t_a, t_b])

    additional = mock.last_params.get("slurm_additional_parameters", {})
    assert additional.get("dependency") == "afterok:1:2"


def test_submit_upstream_without_slurmjob_is_ignored():
    """A skipped upstream (no slurmjob) must not appear in afterok."""
    mock, slurm = _make_executor()
    t_a = _simple_task("a")   # never submitted → slurmjob is None
    t_b = _simple_task("b")

    slurm.submit(t_b, [t_a])

    additional = mock.last_params.get("slurm_additional_parameters", {})
    assert "dependency" not in additional


def test_submit_appends_to_slurmjobs_list():
    mock, slurm = _make_executor()
    slurm.submit(_simple_task("t1"), [])
    slurm.submit(_simple_task("t2"), [])
    assert len(slurm.slurmjobs) == 2


def test_per_task_slurm_parameters_override_defaults():
    mock, slurm = _make_executor()
    t = _simple_task()
    t.slurm_parameters = {"slurm_partition": "cpu", "slurm_time": 30}
    slurm.submit(t, [])
    assert mock.last_params.get("slurm_partition") == "cpu"
    assert mock.last_params.get("slurm_time") == 30


def test_handles_dependencies_returns_true():
    _, slurm = _make_executor()
    assert slurm.handles_dependencies() is True


# ── Task._update_by_slurmjob and status transitions ───────────────────────────
# _update_by_slurmjob calls watcher.update() then reads job.state.
# watcher.update() is a no-op in the mock; tests set job.state directly.

def test_task_status_transitions_pending_running_finished():
    job = MockJob("42", "PENDING")
    t   = _simple_task()
    t.slurmjob = job

    t._update_by_slurmjob()
    assert t._status == TaskStatus.PENDING

    job.state = "RUNNING"
    t._update_by_slurmjob()
    assert t._status == TaskStatus.RUNNING

    job.state = "COMPLETED"
    t._update_by_slurmjob()
    assert t._status == TaskStatus.FINISHED
    assert t.is_in_terminal_state


def test_task_status_failed_state():
    job = MockJob("7", "PENDING")
    t   = _simple_task()
    t.slurmjob = job

    job.state = "FAILED"
    t._update_by_slurmjob()
    assert t._status == TaskStatus.FAILED
    assert t.is_in_failed_terminal_state


def test_task_status_cancelled():
    job = MockJob("8", "PENDING")
    t   = _simple_task()
    t.slurmjob = job

    job.state = "CANCELLED"
    t._update_by_slurmjob()
    assert t._status == TaskStatus.CANCELED


def test_task_status_stays_at_completed():
    job = MockJob("9", "COMPLETED")
    t   = _simple_task()
    t.slurmjob = job

    t._update_by_slurmjob()
    assert t._status == TaskStatus.FINISHED
    t._update_by_slurmjob()          # extra poll — must stay FINISHED
    assert t._status == TaskStatus.FINISHED


# ── Bug regression: status properties must poll on every access ───────────────

def test_slurmjob_status_refreshes_on_every_access():
    """
    Regression: slurmjob_status previously only called _update_by_slurmjob()
    when _slurmstate was None (first access).  Subsequent accesses returned the
    stale state even after the cluster had advanced.
    """
    job = MockJob("1", "PENDING")
    t   = _simple_task()
    t.slurmjob = job

    assert t.slurmjob_status == "PENDING"

    job.state = "RUNNING"                    # cluster advances
    assert t.slurmjob_status == "RUNNING"    # must reflect the change

    job.state = "COMPLETED"
    assert t.slurmjob_status == "COMPLETED"


def test_status_property_refreshes_on_every_access():
    """
    Regression: task.status used the same one-shot guard, so _status
    (and therefore is_in_terminal_state) never updated after the first
    TUI render.
    """
    job = MockJob("2", "PENDING")
    t   = _simple_task()
    t.slurmjob = job

    s, *_ = t.status
    assert s == TaskStatus.PENDING
    assert not t.is_in_terminal_state

    job.state = "RUNNING"
    s, *_ = t.status
    assert s == TaskStatus.RUNNING

    job.state = "COMPLETED"
    s, *_ = t.status
    assert s == TaskStatus.FINISHED
    assert t.is_in_terminal_state


def test_is_in_terminal_state_reflects_slurm_completion():
    """
    After the cluster marks a job COMPLETED, is_in_terminal_state must
    become True so the Pipeline run loop can exit.
    """
    job = MockJob("3", "PENDING")
    t   = _simple_task()
    t.slurmjob = job

    t._update_by_slurmjob()
    assert not t.is_in_terminal_state    # still PENDING

    job.state = "COMPLETED"
    t._update_by_slurmjob()
    assert t.is_in_terminal_state


# ── cancel_all_jobs ────────────────────────────────────────────────────────────

def test_cancel_all_jobs_calls_cancel_on_each():
    cancelled = []

    class TrackingJob(MockJob):
        def cancel(self):
            cancelled.append(self.job_id)

    _, slurm = _make_executor()
    slurm.slurmjobs = [TrackingJob("1"), TrackingJob("2")]
    slurm.cancel_all_jobs()

    assert sorted(cancelled) == ["1", "2"]
