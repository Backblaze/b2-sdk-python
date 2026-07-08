######################################################################
#
# File: test/unit/internal/transfer/test_downloaded_file.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from unittest.mock import Mock

import pytest
from apiver_deps import DownloadedFile
from apiver_deps_exception import ChecksumMismatch, TruncatedOutput


def _generate_downloaded_file(
    *,
    decode_content: bool,
    content_length: int = 100,
    content_sha1: str = 'abc',
    range_: tuple[int, int] | None = None,
    check_hash: bool = True,
):
    download_version = Mock()
    download_version.content_encoding = 'gzip' if decode_content else None
    download_version.content_length = content_length
    download_version.content_sha1 = content_sha1
    download_version.api.api_config.decode_content = decode_content
    download_version._should_be_decoded = decode_content
    return DownloadedFile(
        download_version=download_version,
        download_manager=Mock(),
        range_=range_,
        response=Mock(),
        encryption=None,
        progress_listener=Mock(),
        check_hash=check_hash,
    )


@pytest.mark.parametrize('decode_content', [True, False])
def test_validate_download_truncated_full_download(decode_content):
    # range not set, length doesn't match
    downloaded_file = _generate_downloaded_file(decode_content=decode_content)
    with pytest.raises(TruncatedOutput):
        downloaded_file._validate_download(99, 'abc')


@pytest.mark.parametrize('decode_content', [True, False])
def test_validate_download_truncated_range_download(decode_content):
    # range set, length doesn't match
    downloaded_file = _generate_downloaded_file(decode_content=decode_content, range_=(10, 19))
    with pytest.raises(TruncatedOutput):
        downloaded_file._validate_download(9, 'abc')


@pytest.mark.parametrize('decode_content', [True, False])
def test_validate_download_hash_check(decode_content):
    downloaded_file = _generate_downloaded_file(decode_content=decode_content, check_hash=True)
    if decode_content:
        downloaded_file._validate_download(100, 'wrong')
    else:
        with pytest.raises(ChecksumMismatch):
            downloaded_file._validate_download(100, 'wrong')


@pytest.mark.parametrize('decode_content', [True, False])
def test_validate_download_hash_check_skipped_for_range_download(decode_content):
    downloaded_file = _generate_downloaded_file(
        decode_content=decode_content,
        range_=(10, 19),
        check_hash=True,
    )
    downloaded_file._validate_download(10, 'wrong')
