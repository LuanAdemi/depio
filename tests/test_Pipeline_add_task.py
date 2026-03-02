"""
Tests for Pipeline.add_task(): registration, deduplication, product
conflicts, and dependency ordering.
"""
import pathlib
import pytest

from depio.Pipeline import Pipeline
from depio.Task import Task
from depio.exceptions import ProductAlreadyRegisteredException, TaskNotInQueueException


def _dummy():
    pass


@pytest.fixture
def pipeline():
    return Pipeline(None, quiet=True)


def test_add_task_new_task(pipeline):
    task1 = Task("task1", _dummy)
    pipeline.add_task(task1)
    assert task1 in pipeline.tasks


def test_add_task_duplicated_task(pipeline):
    task1 = Task("task1", _dummy)
    pipeline.add_task(task1)
    pipeline.add_task(task1)
    assert task1 in pipeline.tasks
    assert pipeline.tasks.count(task1) == 1


def test_add_task_duplicate_producing_task(pipeline):
    # Two tasks using the same function with the same args produce the same
    # Task equality → the second is silently dropped (not a conflict).
    producing_task = Task("producing_task", _dummy, produces=[pathlib.Path("test.txt")])
    producing_task2 = Task("producing_task2", _dummy, produces=[pathlib.Path("test.txt")])
    pipeline.add_task(producing_task)
    assert producing_task in pipeline.tasks
    pipeline.add_task(producing_task2)
    # Equal tasks are deduplicated; name is not part of equality.
    assert len(pipeline.tasks) == 1


def test_add_task_unregistered_dependency(pipeline):
    task1 = Task("task1", _dummy)
    task2 = Task("task2", _dummy, depends_on=[task1])
    with pytest.raises(TaskNotInQueueException):
        pipeline.add_task(task2)


def test_add_task_registered_dependency(pipeline):
    task1 = Task("task1", _dummy)
    pipeline.add_task(task1)
    task2 = Task("task2", _dummy, depends_on=[task1])
    pipeline.add_task(task2)
    assert task1 in pipeline.tasks
    assert task2 in pipeline.tasks


def test_add_task_raises_on_duplicate_product(pipeline):
    def producer_a():
        pass

    def producer_b():
        pass

    t1 = Task("t1", producer_a, produces=[pathlib.Path("shared.txt")])
    t2 = Task("t2", producer_b, produces=[pathlib.Path("shared.txt")])
    pipeline.add_task(t1)
    with pytest.raises(ProductAlreadyRegisteredException):
        pipeline.add_task(t2)
