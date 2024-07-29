######################################################################
#
# File: test/unit/internal/transfer/downloader/test_parallel.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import hashlib
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from unittest.mock import Mock

import pytest
from requests import RequestException


def mock_download_response_factory(apiver_module, bucket, file_size: int = 0):
    hasher = hashlib.sha1()

    dummy_data = b"dummy"
    file_content = (dummy_data * (file_size // len(dummy_data) + 1))[:file_size]
    file_version = bucket.upload_bytes(file_content, f"dummy_file_{file_size}.txt")
    hasher.update(file_content)

    url = bucket.api.session.get_download_url_by_name(bucket.name, file_version.file_name)
    response = bucket.api.services.session.download_file_from_url(url).__enter__()

    return response, apiver_module.DownloadVersionFactory(bucket.api).from_response_headers(
        response.headers
    )


@pytest.fixture
def thread_pool():
    with ThreadPoolExecutor(max_workers=10) as executor:
        yield executor


@pytest.fixture
def output_file():
    return BytesIO()


@pytest.fixture
def downloader(apiver_module, thread_pool):
    return apiver_module.ParallelDownloader(
        min_part_size=10,
        force_chunk_size=5,
        thread_pool=thread_pool,
    )


def test_download_empty_file(apiver_module, b2api, bucket, downloader, output_file):
    file_size = 0
    mock_response, download_version = mock_download_response_factory(
        apiver_module, bucket, file_size=file_size
    )
    mock_response.close = Mock(side_effect=mock_response.close)

    bytes_written, hash_hex = downloader.download(
        output_file, mock_response, download_version, b2api.session
    )

    assert bytes_written == file_size
    assert hash_hex == "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    assert output_file.getvalue() == b""
    mock_response.close.assert_called_once()


def test_download_file(apiver_module, b2api, bucket, downloader, output_file):
    file_size = 100
    mock_response, download_version = mock_download_response_factory(
        apiver_module, bucket, file_size=file_size
    )
    mock_response.close = Mock(side_effect=mock_response.close)

    bytes_written, hash_hex = downloader.download(
        output_file, mock_response, download_version, b2api.session
    )

    assert bytes_written == file_size
    assert hash_hex == "7804df8c623573ccfc1993e04981006e5bc30383"
    assert output_file.getvalue() == b"dummy" * 20
    mock_response.close.assert_called_once()


def test_download_file__data_stream_error__in_first_response(
    apiver_module, b2api, bucket, downloader, output_file
):
    """
    Test that the downloader handles a stream error in the first response.
    """
    file_size = 100
    mock_response, download_version = mock_download_response_factory(
        apiver_module, bucket, file_size=file_size
    )

    def iter_content(chunk_size=1, decode_unicode=False):
        yield b"DUMMY"
        raise RequestException("stream error")
        yield  # noqa

    mock_response.iter_content = iter_content

    bytes_written, hash_hex = downloader.download(
        output_file, mock_response, download_version, b2api.session
    )

    assert bytes_written == file_size
    assert output_file.getvalue() == b"DUMMY" + b"dummy" * 19


def test_download_file__data_stream_error__persistent_errors(
    apiver_module, b2api, bucket, downloader, output_file
):
    file_size = 1000
    mock_response, download_version = mock_download_response_factory(
        apiver_module, bucket, file_size=file_size
    )

    # Ensure that follow-up requests also return errors
    def iter_content(chunk_size=1, decode_unicode=False):
        yield b"d"
        raise RequestException("stream error")

    mock_response.iter_content = iter_content

    bucket.api.services.session.download_file_from_url = Mock(return_value=mock_response)

    with pytest.raises(RequestException):
        downloader.download(output_file, mock_response, download_version, b2api.session)


def test_download_file__data_stream_error__multiple_errors_recovery(
    apiver_module, b2api, bucket, downloader, output_file
):
    """Test downloader handles multiple half-failed requests and still downlaods entire file."""
    # This works since each part is attempted up to 15 times before giving up
    file_size = 100
    mock_response, download_version = mock_download_response_factory(
        apiver_module, bucket, file_size=file_size
    )

    def first_iter_content(chunk_size=1, decode_unicode=False):
        yield mock_response.raw.read(1)
        raise RequestException("stream error")

    mock_response.iter_content = first_iter_content

    download_func = bucket.api.services.session.download_file_from_url

    def download_func_mock(*args, **kwargs):
        response = download_func(*args, **kwargs).__enter__()

        def iter_content(chunk_size=1, decode_unicode=False):
            yield response.raw.read(1).upper()
            raise RequestException("stream error")

        response.iter_content = iter_content
        return response

    bucket.api.services.session.download_file_from_url = download_func_mock

    bytes_written, hash_hex = downloader.download(
        output_file, mock_response, download_version, b2api.session
    )

    assert bytes_written == file_size
    assert output_file.getvalue() == b"dUMMY" + b"DUMMY" * 19
