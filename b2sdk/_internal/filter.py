######################################################################
#
# File: b2sdk/_internal/filter.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from enum import Enum
from typing import Sequence


class FilterType(Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


@dataclass
class Filter:
    type: FilterType
    pattern: str

    @classmethod
    def include(cls, pattern: str) -> Filter:
        return cls(type=FilterType.INCLUDE, pattern=pattern)

    @classmethod
    def exclude(cls, pattern: str) -> Filter:
        return cls(type=FilterType.EXCLUDE, pattern=pattern)


class FilterMatcher:
    """
    Holds a list of filters and matches a string (i.e. file name) against them.

    The order of filters matters. The *last* matching filter decides whether
    the string is included or excluded. If no filter matches, the string is
    included by default.

    If the given list of filters contains only INCLUDE filters, then it is
    assumed that all files are excluded by default. In this case, an additional
    EXCLUDE filter is prepended to the list.

    :param filters: list of filters
    """

    def __init__(self, filters: Sequence[Filter]):
        if filters and all(filter_.type == FilterType.INCLUDE for filter_ in filters):
            filters = [Filter(type=FilterType.EXCLUDE, pattern="*"), *filters]

        self.filters = filters

    def match(self, s: str) -> bool:
        include_file = True
        for filter_ in self.filters:
            matched = fnmatch.fnmatchcase(s, filter_.pattern)
            if matched and filter_.type == FilterType.INCLUDE:
                include_file = True
            elif matched and filter_.type == FilterType.EXCLUDE:
                include_file = False

        return include_file
