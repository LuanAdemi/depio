"""
Tests for Task.statuscolor(): maps every known TaskStatus to its colour
string, and raises UnknownStatusException for unrecognised values.
"""
import pytest

from depio.Task import Task, _status_colors
from depio.exceptions import UnknownStatusException


def _fresh():
    return Task("t", lambda: None)


def test_statuscolor_for_all_known_statuses():
    task = _fresh()
    for status, expected_color in _status_colors.items():
        assert task.statuscolor(status) == expected_color, (
            f"Unexpected color for {status}: got {task.statuscolor(status)!r}, "
            f"expected {expected_color!r}"
        )


def test_statuscolor_uses_current_status_when_none_passed():
    task = _fresh()
    from depio.TaskStatus import TaskStatus
    task._status = TaskStatus.RUNNING
    assert task.statuscolor() == _status_colors[TaskStatus.RUNNING]


def test_statuscolor_raises_for_unknown_status():
    task = _fresh()
    with pytest.raises(UnknownStatusException):
        task.statuscolor("unrecognized_status")


def test_statuscolor_raises_for_none_explicitly():
    # Passing an arbitrary non-TaskStatus object that's not in _status_colors
    task = _fresh()
    with pytest.raises(UnknownStatusException):
        task.statuscolor(object())
