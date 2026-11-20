######################################################################
#
# File: test/unit/v_all/test_regression_legacy_api.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for the legacy v0/v1 interfaces.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import apiver_deps
import pytest
from apiver_deps import B2Api, RawSimulator, StubAccountInfo

from test.known_defect import DefectStillPresent, defect_raises, known_defect

if apiver_deps.V <= 1:
    from apiver_deps import DownloadDestBytes
else:
    DownloadDestBytes = None  # not present in v2+; tests are skipped there

pytestmark = [pytest.mark.apiver(to_ver=1)]


@pytest.fixture
def bucket():
    simulator = RawSimulator()
    api = B2Api(StubAccountInfo(), raw_api=simulator)
    account_id, master_key = simulator.create_account()
    api.authorize_account('production', account_id, master_key)
    return api.create_bucket('my-bucket', 'allPublic')


@pytest.mark.apiver(0)
@known_defect('C48', 'v0 unhide_file() raises TypeError', raises=DefectStillPresent)
def test_c48_v0_unhide_file_removes_hide_marker(bucket):
    """C48: on the v0 interface unhide_file() must not fail with a TypeError."""
    bucket.upload_bytes(b'hello', 'hello.txt')
    bucket.hide_file('hello.txt')
    # CPython names the first invalid keyword; PyPy reports both invalid keywords.
    with defect_raises(
        TypeError,
        match=(
            r"^Bucket\.list_file_versions\(\) got (?:an unexpected keyword argument 'file_name'"
            r'|2 unexpected keyword arguments)$'
        ),
    ):
        bucket.unhide_file('hello.txt')
    names = [info.file_name for info, _ in bucket.ls(show_versions=False)]
    assert 'hello.txt' in names, names


@known_defect(
    'C44',
    'v0/v1 clients cannot download a file stored with a Content-Encoding',
    raises=DefectStillPresent,
)
def test_c44_legacy_api_can_download_file_stored_with_content_encoding(bucket):
    """C44: v0/v1 clients must be able to download a file that has a Content-Encoding."""
    data = b'compressed-looking payload'
    bucket.upload_bytes(data, 'enc.bin', file_infos={'b2-content-encoding': 'gzip'})
    dest = DownloadDestBytes()
    with defect_raises(AttributeError, match="no attribute 'api_config'"):
        bucket.download_file_by_name('enc.bin', dest)
    assert dest.get_bytes_written() == data, 'wrong downloaded data'
