######################################################################
#
# File: test/unit/v_all/test_regression_legacy_api.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for the legacy v0/v1 interfaces."""

from __future__ import annotations

import gzip
import hashlib

import apiver_deps
import pytest
import responses
from apiver_deps import (
    DEFAULT_HTTP_API_CONFIG,
    B2Api,
    B2HttpApiConfig,
    B2RawHTTPApi,
    RawSimulator,
    StubAccountInfo,
)

from b2sdk._internal.b2http import B2Http

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
def test_c48_v0_unhide_file_removes_hide_marker(bucket):
    """C48: on the v0 interface unhide_file() must not fail with a TypeError."""
    bucket.upload_bytes(b'hello', 'hello.txt')
    bucket.hide_file('hello.txt')
    bucket.unhide_file('hello.txt')
    names = [info.file_name for info, _ in bucket.ls(show_versions=False)]
    assert 'hello.txt' in names, names


def test_c44_legacy_api_can_download_file_stored_with_content_encoding(bucket):
    """C44: v0/v1 clients must be able to download a file that has a Content-Encoding."""
    data = b'compressed-looking payload'
    bucket.upload_bytes(data, 'enc.bin', file_infos={'b2-content-encoding': 'gzip'})
    dest = DownloadDestBytes()
    bucket.download_file_by_name('enc.bin', dest)
    assert dest.get_bytes_written() == data, 'wrong downloaded data'


@pytest.mark.parametrize('decode_content', [False, True])
def test_legacy_api_retains_explicit_configuration(decode_content):
    config = B2HttpApiConfig(decode_content=decode_content, _raw_api_class=RawSimulator)
    api = B2Api(StubAccountInfo(), api_config=config)
    assert api.api_config is config


@pytest.mark.parametrize('use_raw_api', [False, True])
def test_legacy_api_retains_default_configuration(use_raw_api):
    raw_api = RawSimulator()
    kwargs = {'raw_api': raw_api} if use_raw_api else {}
    api = B2Api(StubAccountInfo(), **kwargs)
    assert api.api_config is DEFAULT_HTTP_API_CONFIG
    if use_raw_api:
        assert api.session.raw_api is raw_api


@pytest.mark.parametrize('decode_content', [False, True])
@pytest.mark.parametrize('by_id', [False, True], ids=['by-name', 'by-id'])
def test_legacy_encoded_http_download_preserves_decode_setting(decode_content, by_id):
    data = b'actual gzip payload' * 100
    encoded = gzip.compress(data, mtime=0)
    config = B2HttpApiConfig(
        decode_content=decode_content,
        install_clock_skew_hook=False,
        _raw_api_class=RawSimulator,
    )
    api = B2Api(StubAccountInfo(), api_config=config)
    simulator = api.session.raw_api
    account_id, master_key = simulator.create_account()
    api.authorize_account('production', account_id, master_key)
    bucket = api.create_bucket('encoded-downloads', 'allPrivate')
    api.session.raw_api = B2RawHTTPApi(B2Http(config))
    if by_id:
        url = api.session.get_download_url_by_id('encoded-file')
    else:
        url = api.session.get_download_url_by_name(bucket.name, 'enc.bin')
    headers = {
        'Content-Length': str(len(encoded)),
        'Content-Encoding': 'gzip',
        'X-Bz-File-Id': 'encoded-file',
        'X-Bz-File-Name': 'enc.bin',
        'X-Bz-Content-Sha1': hashlib.sha1(encoded).hexdigest(),
        'X-Bz-Upload-Timestamp': '1000',
    }
    destination = DownloadDestBytes()
    with responses.RequestsMock() as transport:
        transport.add(
            responses.GET,
            url,
            body=encoded,
            headers=headers,
            content_type='application/octet-stream',
        )
        if by_id:
            result = api.download_file_by_id('encoded-file', destination)
        else:
            result = bucket.download_file_by_name('enc.bin', destination)
    assert destination.get_bytes_written() == (data if decode_content else encoded)
    assert result['fileName'] == 'enc.bin'


def test_legacy_api_still_rejects_raw_api_with_configuration():
    from apiver_deps_exception import InvalidArgument

    with pytest.raises(InvalidArgument, match='Provide at most one'):
        B2Api(StubAccountInfo(), raw_api=RawSimulator(), api_config=B2HttpApiConfig())
