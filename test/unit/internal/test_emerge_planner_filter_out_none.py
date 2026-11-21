######################################################################
#
# File: test/unit/internal/test_emerge_planner_filter_out_none.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest

from b2sdk._internal.http_constants import (
    DEFAULT_MAX_PART_SIZE,
    DEFAULT_MIN_PART_SIZE,
    DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE,
)
from b2sdk._internal.transfer.emerge.planner.planner import EmergePlanner, _filter_out_none


@pytest.mark.parametrize(
    'args, expected',
    [
        ((), []),
        ((None,), []),
        ((None, None), []),
        ((1, 2), [1, 2]),
        ((None, 5), [5]),
        ((5, None), [5]),
        ((3, None, 7, None), [3, 7]),
        # only `None` is dropped; other falsy values must survive
        ((0, None, '', False, []), [0, '', False, []]),
    ],
)
def test_filter_out_none_drops_only_none_and_keeps_order(args, expected):
    assert list(_filter_out_none(*args)) == expected


def test_filter_out_none_is_lazy_and_single_pass():
    result = _filter_out_none(1, None, 2)
    assert next(result) == 1
    assert list(result) == [2]
    assert list(result) == []


def test_planner_min_part_size_skips_the_unset_limit():
    # `_filter_out_none` feeds `min(...)` in the planner constructor: if it let `None`
    # through, `min` would raise a TypeError (or compare None to an int) instead of
    # using the one limit that is set.
    planner = EmergePlanner(recommended_upload_part_size=None, max_part_size=DEFAULT_MAX_PART_SIZE)
    assert planner.min_part_size == DEFAULT_MIN_PART_SIZE
    assert planner.recommended_upload_part_size == DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE
    assert planner.max_part_size == DEFAULT_MAX_PART_SIZE


def test_planner_min_part_size_is_lowered_by_the_configured_limit():
    small = DEFAULT_MIN_PART_SIZE // 2
    planner = EmergePlanner(recommended_upload_part_size=small, max_part_size=None)
    assert planner.min_part_size == small
