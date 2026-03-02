"""
Tests for miscellaneous Task behaviour: __str__, __hash__, id property,
get_duration, get_stdout/get_stderr, statustext, statustext_long, and
the status property (without slurmjob).
"""
import time
import pytest

from depio.Task import Task, _status_texts
from depio.TaskStatus import TaskStatus
from depio.BuildMode import BuildMode
from depio.exceptions import UnknownStatusException


def _dummy():
    pass


def _fresh(name="t"):
    return Task(name, _dummy)


# ---------------------------------------------------------------------------
# __str__
# ---------------------------------------------------------------------------

def test_str_includes_name():
    task = Task("my_task", _dummy)
    assert str(task) == "Task:my_task"


def test_str_format():
    task = Task("x", _dummy)
    assert str(task).startswith("Task:")


# ---------------------------------------------------------------------------
# __hash__
# ---------------------------------------------------------------------------

def _fn(a, b):
    pass


def test_hash_is_stable():
    task = Task("t", _fn, [1, 2])
    assert hash(task) == hash(task)


def test_equal_tasks_have_equal_hash():
    task1 = Task("t", _fn, [1, 2])
    task2 = Task("t", _fn, [1, 2])
    assert task1 == task2
    assert hash(task1) == hash(task2)


def test_different_functions_different_hash():
    def fn2(a, b):
        pass

    task1 = Task("t", _fn, [1, 2])
    task2 = Task("t", fn2, [1, 2])
    assert task1 != task2
    # Hashes may collide, but at least they're ints
    assert isinstance(hash(task1), int)
    assert isinstance(hash(task2), int)


# ---------------------------------------------------------------------------
# id property
# ---------------------------------------------------------------------------

def test_id_without_queue_id():
    task = _fresh()
    assert task.id == "None"


def test_id_with_queue_id():
    task = _fresh()
    task._queue_id = 1
    # Format string is f"{self._queue_id: 4d}"
    assert task.id == "   1"


def test_id_with_larger_queue_id():
    task = _fresh()
    task._queue_id = 42
    assert task.id == "  42"


# ---------------------------------------------------------------------------
# get_duration
# ---------------------------------------------------------------------------

def test_get_duration_before_run_returns_zero():
    task = _fresh()
    assert task.get_duration() == 0


def test_get_duration_after_successful_run():
    def func():
        pass

    task = Task("t", func, buildmode=BuildMode.ALWAYS)
    task.path_dependencies = []
    task.task_dependencies = []
    task.run()

    assert task.get_duration() >= 0


def test_get_duration_with_only_start_time():
    task = _fresh()
    task.start_time = time.time() - 3  # pretend started 3 seconds ago
    # end_time is still None → uses current time
    duration = task.get_duration()
    assert 2 <= duration <= 5  # allow some slack


def test_get_duration_with_start_and_end():
    task = _fresh()
    task.start_time = 1000.0
    task.end_time = 1005.0
    assert task.get_duration() == 5


# ---------------------------------------------------------------------------
# get_stdout / get_stderr (no slurmjob)
# ---------------------------------------------------------------------------

def test_get_stdout_initially_empty():
    task = _fresh()
    assert task.get_stdout() == ""


def test_get_stderr_initially_empty():
    task = _fresh()
    assert task.get_stderr() == ""


def test_get_stdout_returns_str():
    task = _fresh()
    assert isinstance(task.get_stdout(), str)


def test_get_stderr_returns_str():
    task = _fresh()
    assert isinstance(task.get_stderr(), str)


# ---------------------------------------------------------------------------
# statustext
# ---------------------------------------------------------------------------

def test_statustext_for_all_known_statuses():
    task = _fresh()
    for status, expected_text in _status_texts.items():
        assert task.statustext(status) == expected_text


def test_statustext_uses_current_status_when_none_passed():
    task = _fresh()
    task._status = TaskStatus.RUNNING
    assert task.statustext() == _status_texts[TaskStatus.RUNNING]


def test_statustext_raises_for_unknown():
    task = _fresh()
    with pytest.raises(UnknownStatusException):
        task.statustext("not_a_real_status")


# ---------------------------------------------------------------------------
# statustext_long
# ---------------------------------------------------------------------------

def test_statustext_long_returns_string_for_all_statuses():
    task = _fresh()
    task.task_dependencies = []
    for status in _status_texts:
        result = task.statustext_long(status)
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# status property (no slurmjob)
# ---------------------------------------------------------------------------

def test_status_property_returns_tuple():
    task = _fresh()
    s = task.status
    assert isinstance(s, tuple)
    assert len(s) == 4


def test_status_property_first_element_is_task_status():
    task = _fresh()
    status_val, _, _, _ = task.status
    assert isinstance(status_val, TaskStatus)


def test_status_property_reflects_current_status():
    task = _fresh()
    task._status = TaskStatus.RUNNING
    status_val, text, color, slurm = task.status
    assert status_val == TaskStatus.RUNNING


def test_status_property_slurm_empty_without_slurmjob():
    task = _fresh()
    _, _, _, slurm = task.status
    assert slurm == ""
