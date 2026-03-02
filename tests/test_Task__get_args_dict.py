"""
Tests for _get_args_dict(): maps positional and keyword arguments to a
{name: value} dictionary based on the function's parameter names.
"""
from depio.Task import _get_args_dict


def test_positional_args_only():
    def fn(a, b, c):
        pass

    result = _get_args_dict(fn, [1, 2, 3], {})
    assert result == {'a': 1, 'b': 2, 'c': 3}


def test_positional_args_with_extra_kwargs():
    def fn(a, b, c, **kwargs):
        pass

    result = _get_args_dict(fn, [1, 2, 3], {'d': 4, 'e': 5})
    assert result == {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5}


def test_kwargs_order_preserved():
    def fn(a, b, c, **kwargs):
        pass

    result = _get_args_dict(fn, [1, 2, 3], {'d': 5, 'e': 4})
    assert result == {'a': 1, 'b': 2, 'c': 3, 'd': 5, 'e': 4}


def test_mixed_positional_and_kwargs():
    def fn(a, b, c, *args, **kwargs):
        pass

    result = _get_args_dict(fn, [1], {'b': 2, 'c': 3, 'd': 4})
    assert result == {'a': 1, 'b': 2, 'c': 3, 'd': 4}


def test_no_args_no_kwargs():
    def fn():
        pass

    result = _get_args_dict(fn, [], {})
    assert result == {}


def test_kwargs_only():
    def fn(a, b):
        pass

    result = _get_args_dict(fn, [], {'a': 10, 'b': 20})
    assert result == {'a': 10, 'b': 20}
