"""
Tests for _get_not_updated_products(): returns products whose mtime
did not change between the before and after snapshots.
"""
from depio.Task import _get_not_updated_products


def test_no_products_updated():
    before = {'product1': 'ts1', 'product2': 'ts2'}
    after  = {'product1': 'ts1', 'product2': 'ts2'}
    result = _get_not_updated_products(after, before)
    assert result == ['product1', 'product2']


def test_one_product_updated():
    before = {'product1': 'ts1', 'product2': 'ts2'}
    after  = {'product1': 'ts1', 'product2': 'new_ts2'}
    result = _get_not_updated_products(after, before)
    assert result == ['product1']


def test_all_products_updated():
    before = {'product1': 'ts1', 'product2': 'ts2'}
    after  = {'product1': 'new_ts1', 'product2': 'new_ts2'}
    result = _get_not_updated_products(after, before)
    assert result == []


def test_order_of_before_dict_respected():
    before = {'product1': 'ts1', 'product2': 'ts2'}
    after  = {'product2': 'ts2', 'product1': 'ts1'}
    result = _get_not_updated_products(after, before)
    assert result == ['product1', 'product2']


def test_empty_dicts():
    result = _get_not_updated_products({}, {})
    assert result == []


def test_new_product_in_after_is_ignored():
    # A product that appears only in 'after' (brand-new) is not flagged
    before = {'p1': 'ts1'}
    after  = {'p1': 'new_ts1', 'p2': 'ts2'}
    result = _get_not_updated_products(after, before)
    assert result == []
