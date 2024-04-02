######################################################################
#
# File: b2sdk/_internal/included_sources.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

# This module provides a list of third party sources included and modified in b2sdk, so it can be exposed to
# B2 Command Line Tool for printing, for legal compliance reasons
import dataclasses

_included_sources: list[IncludedSourceMeta] = []


@dataclasses.dataclass
class IncludedSourceMeta:
    name: str
    comment: str
    files: dict[str, str]


def add_included_source(src: IncludedSourceMeta):
    _included_sources.append(src)


def get_included_sources() -> list[IncludedSourceMeta]:
    return _included_sources
