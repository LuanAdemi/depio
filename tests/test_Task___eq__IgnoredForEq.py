"""
Tests for Task.__eq__ when parameters are annotated with IgnoredForEq:
those arguments must not affect equality comparison.
"""
from typing import Annotated

from depio.Task import Task, IgnoredForEq


def _func1(x: Annotated[int, IgnoredForEq], y: int):
    pass


def _func2(x: Annotated[int, IgnoredForEq], y: int):
    pass


def test_task_eq_same_funcs():
    task1 = Task("task", _func1)
    task2 = Task("task", _func1)
    assert task1 == task2


def test_task_eq_diff_funcs():
    task1 = Task("task", _func1)
    task2 = Task("task", _func2)
    assert task1 != task2


def test_task_eq_same_args():
    task1 = Task("task", _func1, [1, 2])
    task2 = Task("task", _func1, [1, 2])
    assert task1 == task2


def test_task_eq_diff_non_ignored_arg():
    # y differs → not equal
    task1 = Task("task", _func1, [1, 2])
    task2 = Task("task", _func1, [1, 3])
    assert task1 != task2


def test_task_eq_same_args_with_none():
    task1 = Task("task", _func1, [None, 2])
    task2 = Task("task", _func1, [None, 2])
    assert task1 == task2


def test_task_eq_diff_ignored_arg_is_equal():
    # x is IgnoredForEq → different values still compare as equal
    task1 = Task("task", _func1, [1, 2])
    task2 = Task("task", _func1, [2, 2])
    assert task1 == task2


def test_task_ignored_arg_not_in_cleaned_args():
    task = Task("task", _func1, [42, 7])
    assert "x" not in task.cleaned_args
    assert "y" in task.cleaned_args
