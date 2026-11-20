######################################################################
#
# File: test/unit/test_regression_misc.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for validation helpers, folder scanning, the HTTP layer and types.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import importlib
from unittest import mock

import pytest
import requests
import responses
from apiver_deps import B2Http, B2HttpApiConfig
from apiver_deps_exception import B2ConnectionError, B2RequestTimeout

from test.known_defect import DefectStillPresent, defect_raises, expect, known_defect

pytestmark = [pytest.mark.apiver(from_ver=2)]

URL = 'https://example.com/b2api/v4/test'


def _b2_http():
    return B2Http(B2HttpApiConfig(requests.Session, install_clock_skew_hook=False))


class TestTypes:
    @known_defect(
        'C31',
        'the pydantic version is compared as text, so "10.0" counts as older than "2"',
        raises=AssertionError,
    )
    def test_c31_pydantic_version_ten_counts_as_v2_or_newer(self, monkeypatch):
        """C31: pydantic version comparison must be numeric ('10.0' is newer than '2')."""
        pydantic = pytest.importorskip('pydantic')
        types_module = importlib.import_module('b2sdk._internal.types')
        monkeypatch.setattr(pydantic, '__version__', '10.0.0')
        try:
            importlib.reload(types_module)
            detected = types_module.pydantic
        finally:
            monkeypatch.undo()
            importlib.reload(types_module)
        assert detected is not None, 'pydantic was not detected'


class TestHttpLayer:
    @responses.activate
    def test_c10_create_style_post_is_not_blindly_resent_after_timeout(self):
        """C10: a POST that creates something (b2_create_bucket) must not be re-sent after a timeout."""
        url = 'https://example.com/b2api/v4/b2_create_bucket'
        responses.add(responses.POST, url, body=requests.exceptions.ReadTimeout('timed out'))
        http = _b2_http()
        with mock.patch('time.sleep'):
            with pytest.raises(B2RequestTimeout):
                http.post_json_return_json(url, {}, {'bucketName': 'b'})
        expect(len(responses.calls) == 1, f'request was sent {len(responses.calls)} times')

    @pytest.mark.parametrize('args', [()])
    @known_defect(
        'C51',
        'error mapping raises IndexError for a ConnectionError with no arguments',
        raises=DefectStillPresent,
    )
    def test_c51_connection_error_with_missing_args_is_mapped_without_indexerror(self, args):
        """C51: ConnectionError carrying fewer than 2 args must map to a B2 error, not IndexError."""
        from b2sdk._internal.b2http import B2Http as InternalB2Http

        error = requests.ConnectionError(*args)
        with defect_raises(IndexError, match='tuple index out of range'):
            with pytest.raises(B2ConnectionError):
                InternalB2Http._translate_errors(lambda: (_ for _ in ()).throw(error))
