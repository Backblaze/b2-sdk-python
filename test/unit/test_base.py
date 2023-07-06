######################################################################
#
# File: test/unit/test_base.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import re
import unittest
from contextlib import contextmanager

import apiver_deps
from apiver_deps import B2Api

from b2sdk.v2 import FullApplicationKey


class TestBase(unittest.TestCase):
    @contextmanager
    def assertRaises(self, exc, msg=None):
        try:
            yield
        except exc as e:
            if msg is not None:
                if msg != str(e):
                    assert False, f"expected message '{msg}', but got '{str(e)}'"
        else:
            assert False, f'should have thrown {exc}'

    @contextmanager
    def assertRaisesRegexp(self, expected_exception, expected_regexp):
        try:
            yield
        except expected_exception as e:
            if not re.search(expected_regexp, str(e)):
                assert False, f"expected message '{expected_regexp}', but got '{str(e)}'"
        else:
            assert False, f'should have thrown {expected_exception}'


def create_key(
    api: B2Api,
    capabilities: list[str],
    key_name: str,
    valid_duration_seconds: int | None = None,
    bucket_id: str | None = None,
    name_prefix: str | None = None,
) -> FullApplicationKey:
    """apiver-agnostic B2Api.create_key"""
    result = api.create_key(
        capabilities=capabilities,
        key_name=key_name,
        valid_duration_seconds=valid_duration_seconds,
        bucket_id=bucket_id,
        name_prefix=name_prefix,
    )
    if apiver_deps.V <= 1:
        return FullApplicationKey.from_create_response(result)
    return result
