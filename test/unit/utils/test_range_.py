######################################################################
#
# File: test/unit/utils/test_range_.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest


def test_range_initialization(apiver_module):
    r = apiver_module.Range(0, 10)
    assert r.start == 0
    assert r.end == 10


def test_range_eq(apiver_module):
    r = apiver_module.Range(5, 10)
    assert r == apiver_module.Range(5, 10)
    assert r != apiver_module.Range(5, 11)
    assert r != apiver_module.Range(6, 10)


def test_range_initialization_invalid(apiver_module):
    with pytest.raises(AssertionError):
        apiver_module.Range(10, 0)


def test_range_from_header(apiver_module):
    r = apiver_module.Range.from_header("bytes=0-11")
    assert r.start == 0
    assert r.end == 11


@pytest.mark.parametrize(
    "raw_range_header, start, end, total_length", [
        ("bytes 0-11", 0, 11, None),
        ("bytes 1-11/*", 1, 11, None),
        ("bytes 10-110/200", 10, 110, 200),
    ]
)
def test_range_from_header_with_size(apiver_module, raw_range_header, start, end, total_length):
    r, length = apiver_module.Range.from_header_with_size(raw_range_header)
    assert r.start == start
    assert r.end == end
    assert length == total_length


def test_range_size(apiver_module):
    r = apiver_module.Range(0, 10)
    assert r.size() == 11


def test_range_subrange(apiver_module):
    r = apiver_module.Range(1, 10)
    assert r.subrange(0, 9) == apiver_module.Range(1, 10)
    assert r.subrange(2, 5) == apiver_module.Range(3, 6)


def test_range_subrange_invalid(apiver_module):
    r = apiver_module.Range(0, 10)
    with pytest.raises(AssertionError):
        r.subrange(5, 15)


def test_range_as_tuple(apiver_module):
    r = apiver_module.Range(0, 10)
    assert r.as_tuple() == (0, 10)


def test_range_repr(apiver_module):
    r = apiver_module.Range(0, 10)
    assert repr(r) == "Range(0, 10)"


def test_empty_range(apiver_module):
    r = apiver_module.EMPTY_RANGE
    assert r.size() == 0
