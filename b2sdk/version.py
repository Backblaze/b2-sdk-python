######################################################################
#
# File: b2sdk/version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from sys import version_info as _version_info

try:
    from importlib.metadata import version as _version
except ModuleNotFoundError:  # python 3.7
    from importlib_metadata import version as _version

__all__ = [
    "VERSION",
    "PYTHON_VERSION",
    "USER_AGENT",
]

VERSION = _version("b2sdk")

PYTHON_VERSION = ".".join(map(str, _version_info[:3]))  # something like: 3.9.1

USER_AGENT = f"backblaze-b2/{VERSION} python/{PYTHON_VERSION}"
