######################################################################
#
# File: test/unit/test_base.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from contextlib import contextmanager
from typing import List, Optional
import re
import unittest

import apiver_deps
from apiver_deps import B2Api
from b2sdk._v2 import FullApplicationKey


class TestBase(unittest.TestCase):
    @contextmanager
    def assertRaises(self, exc, msg=None):
        try:
            yield
        except exc as e:
            if msg is not None:
                if msg != str(e):
                    assert False, "expected message '%s', but got '%s'" % (msg, str(e))
        else:
            assert False, 'should have thrown %s' % (exc,)

    @contextmanager
    def assertRaisesRegexp(self, expected_exception, expected_regexp):
        try:
            yield
        except expected_exception as e:
            if not re.search(expected_regexp, str(e)):
                assert False, "expected message '%s', but got '%s'" % (expected_regexp, str(e))
        else:
            assert False, 'should have thrown %s' % (expected_exception,)


def create_key(
    api: B2Api,
    capabilities: List[str],
    key_name: str,
    valid_duration_seconds: Optional[int] = None,
    bucket_id: Optional[str] = None,
    name_prefix: Optional[str] = None,
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
