######################################################################
#
# File: b2sdk/_internal/utils/range_.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import dataclasses
import re

_RANGE_HEADER_RE = re.compile(
    r'^(?:bytes[ =])?(?P<start>\d+)-(?P<end>\d+)(?:/(?:(?P<complete_length>\d+)|\*))?$'
)


@dataclasses.dataclass(eq=True, order=True, frozen=True)
class Range:
    """
    HTTP ranges use an *inclusive* index at the end.
    """
    __slots__ = ['start', 'end']

    start: int
    end: int

    def __post_init__(self):
        assert 0 <= self.start <= self.end or (
            self.start == 1 and self.end == 0
        ), f'Invalid range: {self}'

    @classmethod
    def from_header(cls, raw_range_header: str) -> Range:
        """
        Factory method which returns an object constructed from Range http header.

        raw_range_header example: 'bytes=0-11'
        """
        return cls.from_header_with_size(raw_range_header)[0]

    @classmethod
    def from_header_with_size(cls, raw_range_header: str) -> tuple[Range, int | None]:
        """
        Factory method which returns an object constructed from Range http header.

        raw_range_header example: 'bytes=0-11'
        """
        match = _RANGE_HEADER_RE.match(raw_range_header)
        if not match:
            raise ValueError(f'Invalid range header: {raw_range_header}')
        start = int(match.group('start'))
        end = int(match.group('end'))
        complete_length = match.group('complete_length')
        complete_length = int(complete_length) if complete_length else None
        return cls(start, end), complete_length

    def size(self) -> int:
        return self.end - self.start + 1

    def subrange(self, sub_start, sub_end) -> Range:
        """
        Return a range that is part of this range.

        :param sub_start: index relative to the start of this range.
        :param sub_end: (Inclusive!) index relative to the start of this range.
        :return: a new Range
        """
        assert 0 <= sub_start <= sub_end < self.size()
        return self.__class__(self.start + sub_start, self.start + sub_end)

    def as_tuple(self) -> tuple[int, int]:
        return self.start, self.end

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.start}, {self.end})'


EMPTY_RANGE = Range(1, 0)
