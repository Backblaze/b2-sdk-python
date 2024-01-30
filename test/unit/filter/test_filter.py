######################################################################
#
# File: test/unit/filter/test_filter.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from apiver_deps import Filter

from b2sdk.filter import FilterMatcher


@pytest.mark.parametrize(
    ("filters", "expr", "expected"),
    (
        ([], "a", True),
        ([Filter.exclude("*")], "something", False),
        ([Filter.include("a-*")], "a-", True),
        ([Filter.include("a-*")], "b-", False),
        ([Filter.exclude("*.txt")], "a.txt", False),
        ([Filter.exclude("*.txt")], "a.csv", True),
        ([Filter.exclude("*"), Filter.include("*.[ct]sv")], "a.csv", True),
        ([Filter.exclude("*"), Filter.include("*.[ct]sv")], "a.tsv", True),
        ([Filter.exclude("*"), Filter.include("*.[ct]sv")], "a.ksv", False),
        (
            [Filter.exclude("*"),
             Filter.include("*.[ct]sv"),
             Filter.exclude("a.csv")], "a.csv", False
        ),
        ([Filter.exclude("*"),
          Filter.include("*.[ct]sv"),
          Filter.exclude("a.csv")], "b.csv", True),
    ),
)
def test_filter_matcher(filters, expr, expected):
    assert FilterMatcher(filters).match(expr) == expected
