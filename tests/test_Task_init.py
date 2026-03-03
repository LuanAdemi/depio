"""
Tests for Task.__init__: default values, annotation-driven product/dependency/IgnoredForEq
detection, explicit produces/depends_on, and arg_resolver.
"""
import pytest
from pathlib import Path
from typing import Annotated, List

from depio.Task import Task, Product, Dependency, IgnoredForEq
from depio.TaskStatus import TaskStatus
from depio.BuildMode import BuildMode


# ---------------------------------------------------------------------------
# Helper functions (module-level so they have stable __code__ objects)
# ---------------------------------------------------------------------------

def _dummy():
    pass


def _func_with_product(out: Annotated[Path, Product]):
    pass


def _func_with_dependency(dep: Annotated[Path, Dependency]):
    pass


def _func_with_ignored(x: Annotated[int, IgnoredForEq], y: int):
    pass


def _func_with_list_products(outputs: Annotated[List[Path], Product]):
    pass


def _func_with_list_deps(inputs: Annotated[List[Path], Dependency]):
    pass


def _func_combined(dep: Annotated[Path, Dependency], out: Annotated[Path, Product]):
    pass


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

def test_default_status():
    task = Task("t", _dummy)
    assert task._status == TaskStatus.WAITING


def test_default_buildmode():
    task = Task("t", _dummy)
    assert task.buildmode == BuildMode.IF_MISSING


def test_default_description_empty():
    task = Task("t", _dummy)
    assert task.description == ""


def test_custom_description():
    task = Task("t", _dummy, description="hello")
    assert task.description == "hello"


def test_default_products_empty():
    task = Task("t", _dummy)
    assert task.products == []


def test_default_dependencies_empty():
    task = Task("t", _dummy)
    assert task.dependencies == []


def test_path_and_task_dependencies_initially_none():
    task = Task("t", _dummy)
    assert task.path_dependencies is None
    assert task.task_dependencies is None


def test_dependent_tasks_initially_empty():
    task = Task("t", _dummy)
    assert task.dependent_tasks == []


def test_slurmjob_initially_none():
    task = Task("t", _dummy)
    assert task.slurmjob is None


def test_func_is_stored():
    task = Task("t", _dummy)
    assert task.func is _dummy


def test_name_is_stored():
    task = Task("my_task", _dummy)
    assert task.name == "my_task"


# ---------------------------------------------------------------------------
# Annotation-driven product detection
# ---------------------------------------------------------------------------

def test_product_from_annotation(tmp_path):
    out = tmp_path / "out.txt"
    task = Task("t", _func_with_product, func_args=[out])
    assert out in task.products


def test_dependency_from_annotation(tmp_path):
    dep = tmp_path / "dep.txt"
    task = Task("t", _func_with_dependency, func_args=[dep])
    assert dep in task.dependencies


def test_ignored_for_eq_not_in_cleaned_args():
    task = Task("t", _func_with_ignored, func_args=[42, 7])
    assert "x" not in task.cleaned_args
    assert "y" in task.cleaned_args
    assert task.cleaned_args["y"] == 7


def test_list_products_from_annotation(tmp_path):
    out1, out2 = tmp_path / "a.txt", tmp_path / "b.txt"
    task = Task("t", _func_with_list_products, func_args=[[out1, out2]])
    assert out1 in task.products
    assert out2 in task.products


def test_list_deps_from_annotation(tmp_path):
    dep1, dep2 = tmp_path / "d1.txt", tmp_path / "d2.txt"
    task = Task("t", _func_with_list_deps, func_args=[[dep1, dep2]])
    assert dep1 in task.dependencies
    assert dep2 in task.dependencies


def test_combined_dep_and_product_from_annotation(tmp_path):
    dep = tmp_path / "dep.txt"
    out = tmp_path / "out.txt"
    task = Task("t", _func_combined, func_args=[dep, out])
    assert dep in task.dependencies
    assert out in task.products


# ---------------------------------------------------------------------------
# Explicit produces / depends_on
# ---------------------------------------------------------------------------

def test_explicit_produces(tmp_path):
    out = tmp_path / "out.txt"
    task = Task("t", _dummy, produces=[out])
    assert out in task.products


def test_explicit_depends_on_path(tmp_path):
    dep = tmp_path / "dep.txt"
    task = Task("t", _dummy, depends_on=[dep])
    assert dep in task.dependencies


def test_explicit_depends_on_task():
    dep_task = Task("dep", _dummy)
    task = Task("t", _dummy, depends_on=[dep_task])
    assert dep_task in task.dependencies


def test_annotation_and_explicit_produces_combined(tmp_path):
    out_ann = tmp_path / "ann.txt"
    out_exp = tmp_path / "exp.txt"
    task = Task("t", _func_with_product, func_args=[out_ann], produces=[out_exp])
    assert out_ann in task.products
    assert out_exp in task.products


# ---------------------------------------------------------------------------
# None values in annotated args are excluded from products/deps
# ---------------------------------------------------------------------------

def test_none_product_arg_excluded():
    task = Task("t", _func_with_product, func_args=[None])
    assert None not in task.products
    assert task.products == []


def test_none_dependency_arg_excluded():
    task = Task("t", _func_with_dependency, func_args=[None])
    assert task.dependencies == []


# ---------------------------------------------------------------------------
# arg_resolver
# ---------------------------------------------------------------------------

def test_arg_resolver_is_called():
    called = []

    def resolver(func, args, kwargs):
        called.append(True)
        return [1, 2], {}

    def fn(a, b):
        pass

    Task("t", fn, arg_resolver=resolver)
    assert called == [True]


def test_arg_resolver_args_are_used():
    def resolver(func, args, kwargs):
        return [99], {}

    def fn(a):
        pass

    task = Task("t", fn, arg_resolver=resolver)
    assert task.func_args == [99]
