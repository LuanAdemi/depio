"""
Tests for Task.__eq__: equality is based on function identity and
argument values (name is not part of equality).
"""
from depio.Task import Task


def _func1(a: int, b: int, c: int):
    pass


def _func2(a: int, b: int, c: int):
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
    task1 = Task("task", _func1, [1, 2, 3])
    task2 = Task("task", _func1, [1, 2, 3])
    assert task1 == task2


def test_task_eq_diff_args():
    task1 = Task("task", _func1, [1, 2, 3])
    task2 = Task("task", _func1, [1, 2, 4])
    assert task1 != task2


def test_task_eq_same_args_with_none():
    task1 = Task("task", _func1, [None, 2, 3])
    task2 = Task("task", _func1, [None, 2, 3])
    assert task1 == task2


def test_task_eq_diff_args_with_none():
    task1 = Task("task", _func1, [None, 2, 3])
    task2 = Task("task", _func1, [None, 2, 4])
    assert task1 != task2


def test_task_eq_same_kwargs():
    task1 = Task("task", _func1, None, {'a': 1, 'b': 2})
    task2 = Task("task", _func1, None, {'a': 1, 'b': 2})
    assert task1 == task2


def test_task_eq_diff_kwargs():
    task1 = Task("task", _func1, None, {'a': 1, 'b': 2})
    task2 = Task("task", _func1, None, {'a': 1, 'b': 3})
    assert task1 != task2


def test_task_eq_name_is_ignored():
    # Different names, same function and args → equal
    task1 = Task("task1", _func1)
    task2 = Task("task2", _func1)
    assert task1 == task2


def test_task_neq_non_task_object():
    task = Task("task", _func1)
    assert task != "not a task"
    assert task != 42
    assert task != None  # noqa: E711
