"""
Tests for Pipeline helper methods: add_tasks, _get_non_terminal_tasks,
_get_pending_tasks, and queue_id assignment.
"""
import pytest

from depio.Pipeline import Pipeline
from depio.Task import Task
from depio.TaskStatus import TaskStatus, TERMINAL_STATES


def _dummy():
    pass


@pytest.fixture
def pipeline():
    return Pipeline(None, quiet=True)


# ---------------------------------------------------------------------------
# add_tasks (batch)
# ---------------------------------------------------------------------------

def test_add_tasks_adds_all(pipeline):
    def fn1():
        pass

    def fn2():
        pass

    t1 = Task("t1", fn1)
    t2 = Task("t2", fn2)
    pipeline.add_tasks([t1, t2])
    assert t1 in pipeline.tasks
    assert t2 in pipeline.tasks


def test_add_tasks_empty_list(pipeline):
    pipeline.add_tasks([])
    assert pipeline.tasks == []


def test_add_tasks_deduplicates(pipeline):
    t = Task("t", _dummy)
    pipeline.add_tasks([t, t])
    assert pipeline.tasks.count(t) == 1


# ---------------------------------------------------------------------------
# queue_id assignment
# ---------------------------------------------------------------------------

def test_queue_id_assigned_on_add(pipeline):
    def fn1():
        pass

    def fn2():
        pass

    t1 = Task("t1", fn1)
    t2 = Task("t2", fn2)
    pipeline.add_task(t1)
    pipeline.add_task(t2)
    assert t1._queue_id == 1
    assert t2._queue_id == 2


def test_add_task_returns_task(pipeline):
    t = Task("t", _dummy)
    result = pipeline.add_task(t)
    assert result is t


# ---------------------------------------------------------------------------
# _get_non_terminal_tasks
# ---------------------------------------------------------------------------

def test_get_non_terminal_tasks_waiting(pipeline):
    t = Task("t", _dummy)
    pipeline.add_task(t)
    # WAITING is not terminal
    result = pipeline._get_non_terminal_tasks()
    assert t in result


def test_get_non_terminal_tasks_excludes_all_terminal_states(pipeline):
    for terminal_status in TERMINAL_STATES:
        def fn():
            pass

        t = Task("t", fn)
        pipeline.add_task(t)
        t._status = terminal_status

        result = pipeline._get_non_terminal_tasks()
        assert t not in result

        pipeline.tasks.clear()
        pipeline.registered_products.clear()


def test_get_non_terminal_tasks_mixed(pipeline):
    def fa():
        pass

    def fb():
        pass

    t_running = Task("r", fa)
    t_done = Task("d", fb)
    pipeline.add_task(t_running)
    pipeline.add_task(t_done)
    t_running._status = TaskStatus.RUNNING
    t_done._status = TaskStatus.FINISHED

    result = pipeline._get_non_terminal_tasks()
    assert t_running in result
    assert t_done not in result


# ---------------------------------------------------------------------------
# _get_pending_tasks
# ---------------------------------------------------------------------------

def test_get_pending_tasks_includes_pending(pipeline):
    t = Task("t", _dummy)
    pipeline.add_task(t)
    t._status = TaskStatus.PENDING
    result = pipeline._get_pending_tasks()
    assert t in result


def test_get_pending_tasks_includes_unknown(pipeline):
    t = Task("t", _dummy)
    pipeline.add_task(t)
    t._status = TaskStatus.UNKNOWN
    result = pipeline._get_pending_tasks()
    assert t in result


def test_get_pending_tasks_excludes_waiting(pipeline):
    t = Task("t", _dummy)
    pipeline.add_task(t)
    # default _status = WAITING
    result = pipeline._get_pending_tasks()
    assert t not in result


def test_get_pending_tasks_excludes_running(pipeline):
    t = Task("t", _dummy)
    pipeline.add_task(t)
    t._status = TaskStatus.RUNNING
    result = pipeline._get_pending_tasks()
    assert t not in result


def test_get_pending_tasks_excludes_finished(pipeline):
    t = Task("t", _dummy)
    pipeline.add_task(t)
    t._status = TaskStatus.FINISHED
    result = pipeline._get_pending_tasks()
    assert t not in result
