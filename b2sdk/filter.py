######################################################################
#
# File: b2sdk/filter.py
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


def include(pattern: str) -> Filter:
    return Filter(type=FilterType.INCLUDE, pattern=pattern)


def exclude(pattern: str) -> Filter:
    return Filter(type=FilterType.EXCLUDE, pattern=pattern)


class FilterMatcher:
    def __init__(self, filters: Sequence[Filter] | None = None):
        if not filters:
            filters = []
        elif all(filter_.type == FilterType.INCLUDE for filter_ in filters):
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
