######################################################################
#
# File: test/unit/v_all/test_download_version.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest
from apiver_deps import (
    EMPTY_RANGE,
    SSE_NONE,
    B2Api,
    B2HttpApiConfig,
    DownloadVersion,
    StubAccountInfo,
)


def _make_download_version(api, *, content_encoding):
    return DownloadVersion(
        api=api,
        id_='file_id',
        file_name='file_name',
        size=100,
        content_type='text/plain',
        content_sha1='abc',
        file_info={},
        upload_timestamp=0,
        server_side_encryption=SSE_NONE,
        range_=EMPTY_RANGE,
        content_disposition=None,
        content_length=100,
        content_language=None,
        expires=None,
        cache_control=None,
        content_encoding=content_encoding,
    )


@pytest.mark.parametrize(
    'content_encoding, decode_content, expected',
    [
        (None, False, False),
        (None, True, False),
        ('gzip', False, False),
        ('gzip', True, True),
    ],
)
def test_should_be_decoded(content_encoding, decode_content, expected):
    api = B2Api(StubAccountInfo(), api_config=B2HttpApiConfig(decode_content=decode_content))
    download_version = _make_download_version(api, content_encoding=content_encoding)
    assert download_version._should_be_decoded is expected


@pytest.mark.apiver(to_ver=1)
@pytest.mark.parametrize(
    'content_encoding, decode_content, expected',
    [
        (None, False, False),
        ('gzip', True, True),
    ],
)
def test_should_be_decoded_without_api_config_kwarg(content_encoding, decode_content, expected):
    api = B2Api(StubAccountInfo())
    api.api_config.decode_content = decode_content
    download_version = _make_download_version(api, content_encoding=content_encoding)
    assert download_version._should_be_decoded is expected
