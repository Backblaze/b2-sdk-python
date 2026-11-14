######################################################################
#
# File: test/unit/internal/transfer/downloader/test_simple.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
from collections.abc import Callable, Iterator
from io import BytesIO
from itertools import count
from types import ModuleType
from typing import Any

import pytest
from apiver_deps import B2Api, Bucket, DownloadVersion, SimpleDownloader
from requests.exceptions import ChunkedEncodingError, ConnectionError, ContentDecodingError
from requests.models import Response
from urllib3.exceptions import DecodeError, IncompleteRead, ProtocolError, ReadTimeoutError

CHUNKED_ENCODING_ERROR = ChunkedEncodingError(
    ProtocolError(
        'Connection broken: IncompleteRead(1 bytes read, 99 more expected)',
        IncompleteRead(1, 99),
    )
)
CONTENT_DECODING_ERROR = ContentDecodingError(
    DecodeError('Error -3 while decompressing data: incorrect header check')
)
CONNECTION_ERROR = ConnectionError(ReadTimeoutError(None, None, 'Read timed out.'))


@pytest.fixture
def file_size() -> int:
    return 100


@pytest.fixture
def file_content(file_size: int) -> bytes:
    return os.urandom(file_size)


@pytest.fixture
def mock_download_response(
    apiver_module: ModuleType,
    bucket: Bucket,
    file_content: bytes,
) -> tuple[Response, DownloadVersion]:
    file_version = bucket.upload_bytes(file_content, f'dummy_file_{len(file_content)}.txt')

    url = bucket.api.session.get_download_url_by_name(bucket.name, file_version.file_name)
    response = bucket.api.services.session.download_file_from_url(url).__enter__()

    return (
        response,
        apiver_module.DownloadVersionFactory(bucket.api).from_response_headers(response.headers),
    )


@pytest.fixture
def output_file() -> BytesIO:
    return BytesIO()


@pytest.fixture
def downloader(apiver_module: ModuleType) -> SimpleDownloader:
    return apiver_module.SimpleDownloader(force_chunk_size=5)


def _make_iter_content(
    response: Response,
    attempts: Iterator[int],
    fail_count: int,
    stream_error: ChunkedEncodingError | ConnectionError | ContentDecodingError,
) -> Callable[..., Iterator[bytes]]:
    def iter_content(chunk_size: int = 1, decode_unicode: bool = False) -> Iterator[bytes]:
        attempt = next(attempts)
        chunk = response.raw.read(1)
        if chunk:
            yield chunk
        if attempt <= fail_count:
            raise stream_error
        while True:
            chunk = response.raw.read(chunk_size)
            if not chunk:
                break
            yield chunk

    return iter_content


@pytest.mark.parametrize('fail_count', [0, 1, 2, 4, 5])
@pytest.mark.parametrize(
    'stream_error',
    [
        pytest.param(CHUNKED_ENCODING_ERROR, id='ChunkedEncodingError'),
        pytest.param(CONNECTION_ERROR, id='ConnectionError'),
        pytest.param(CONTENT_DECODING_ERROR, id='ContentDecodingError'),
    ],
)
def test_download_file__stream_read_error(
    b2api: B2Api,
    bucket: Bucket,
    downloader: SimpleDownloader,
    output_file: BytesIO,
    file_size: int,
    file_content: bytes,
    mock_download_response: tuple[Response, DownloadVersion],
    fail_count: int,
    stream_error: ChunkedEncodingError | ConnectionError | ContentDecodingError,
) -> None:
    mock_response, download_version = mock_download_response

    attempts = count(1)
    mock_response.iter_content = _make_iter_content(
        mock_response, attempts, fail_count, stream_error
    )

    download_func = bucket.api.services.session.download_file_from_url

    def download_func_mock(*args: Any, **kwargs: Any) -> Response:
        response = download_func(*args, **kwargs).__enter__()
        response.iter_content = _make_iter_content(response, attempts, fail_count, stream_error)
        return response

    bucket.api.services.session.download_file_from_url = download_func_mock

    bytes_written, _ = downloader.download(
        output_file, mock_response, download_version, b2api.session
    )

    if fail_count < 5:
        assert bytes_written == file_size
        assert output_file.getvalue() == file_content
    else:
        assert bytes_written == fail_count


@pytest.mark.parametrize(
    'stream_error',
    [
        pytest.param(CHUNKED_ENCODING_ERROR, id='ChunkedEncodingError'),
        pytest.param(CONNECTION_ERROR, id='ConnectionError'),
        pytest.param(CONTENT_DECODING_ERROR, id='ContentDecodingError'),
    ],
)
def test_download_file__decoded_stream_stream_read_error_reraises(
    b2api: B2Api,
    bucket: Bucket,
    downloader: SimpleDownloader,
    output_file: BytesIO,
    file_content: bytes,
    mock_download_response: tuple[Response, DownloadVersion],
    stream_error: ChunkedEncodingError | ConnectionError | ContentDecodingError,
) -> None:
    """
    Test that a stream read error during a decoded stream download is re-raised and not retried
    """

    mock_response, download_version = mock_download_response
    download_version.content_encoding = 'gzip'
    download_version.api.api_config.decode_content = True

    attempts = count(1)
    mock_response.iter_content = _make_iter_content(mock_response, attempts, 1, stream_error)

    followup_calls = 0
    download_func = bucket.api.services.session.download_file_from_url

    def download_func_mock(*args: Any, **kwargs: Any) -> Response:
        nonlocal followup_calls
        followup_calls += 1
        response = download_func(*args, **kwargs).__enter__()
        response.iter_content = _make_iter_content(response, attempts, 1, stream_error)
        return response

    bucket.api.services.session.download_file_from_url = download_func_mock

    with pytest.raises(type(stream_error)):
        downloader.download(output_file, mock_response, download_version, b2api.session)

    assert followup_calls == 0
