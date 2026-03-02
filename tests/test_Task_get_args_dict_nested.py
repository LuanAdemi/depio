"""
Tests for _get_args_dict_nested: list arguments are expanded into
name_0, name_1, ... keys alongside the original key.
"""
from depio.Task import _get_args_dict_nested


# ---------------------------------------------------------------------------
# Non-list arguments: behaves like _get_args_dict
# ---------------------------------------------------------------------------

def test_no_lists_plain_args():
    def fn(a, b, c):
        pass

    result = _get_args_dict_nested(fn, [1, 2, 3], {})
    assert result == {"a": 1, "b": 2, "c": 3}


def test_no_lists_with_kwargs():
    def fn(a, b):
        pass

    result = _get_args_dict_nested(fn, [1], {"b": 99})
    assert result == {"a": 1, "b": 99}


# ---------------------------------------------------------------------------
# Single list argument is expanded
# ---------------------------------------------------------------------------

def test_single_list_arg_expanded():
    def fn(items):
        pass

    result = _get_args_dict_nested(fn, [[10, 20, 30]], {})
    assert result["items"] == [10, 20, 30]
    assert result["items_0"] == 10
    assert result["items_1"] == 20
    assert result["items_2"] == 30


def test_single_list_arg_original_key_preserved():
    def fn(items):
        pass

    result = _get_args_dict_nested(fn, [["a", "b"]], {})
    assert "items" in result
    assert result["items"] == ["a", "b"]


# ---------------------------------------------------------------------------
# Multiple list arguments
# ---------------------------------------------------------------------------

def test_multiple_list_args_both_expanded():
    def fn(xs, ys):
        pass

    result = _get_args_dict_nested(fn, [[1, 2], [3, 4]], {})
    assert result["xs_0"] == 1
    assert result["xs_1"] == 2
    assert result["ys_0"] == 3
    assert result["ys_1"] == 4


# ---------------------------------------------------------------------------
# Mixed list and scalar arguments
# ---------------------------------------------------------------------------

def test_mixed_list_and_scalar():
    def fn(paths, flag):
        pass

    result = _get_args_dict_nested(fn, [["p1", "p2"], True], {})
    assert result["paths"] == ["p1", "p2"]
    assert result["paths_0"] == "p1"
    assert result["paths_1"] == "p2"
    assert result["flag"] is True
    assert "flag_0" not in result


# ---------------------------------------------------------------------------
# Empty list: produces no expanded keys
# ---------------------------------------------------------------------------

def test_empty_list_no_expanded_keys():
    def fn(items):
        pass

    result = _get_args_dict_nested(fn, [[]], {})
    assert result["items"] == []
    assert "items_0" not in result


# ---------------------------------------------------------------------------
# Single-element list
# ---------------------------------------------------------------------------

def test_single_element_list():
    def fn(items):
        pass

    result = _get_args_dict_nested(fn, [["only"]], {})
    assert result["items_0"] == "only"
    assert "items_1" not in result
