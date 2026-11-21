######################################################################
#
# File: test/unit/stream/test_upload_source_safety.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import io
import logging

import pytest

from b2sdk._internal.transfer.outbound.upload_source import (
    UploadSourceBytes,
    UploadSourceLocalFileRange,
    UploadSourceStream,
)


@pytest.mark.parametrize(
    'payload', [b'sample-data', b'sample-data-' * 5, bytearray(b'sample-data')]
)
def test_bytes_source_repr_and_logs_do_not_expose_payload(payload, caplog):
    source = UploadSourceBytes(payload)
    logger = logging.getLogger('test.upload-source')
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        logger.debug('upload source: %r', source)
    assert 'sample-data' not in repr(source)
    assert caplog.records
    assert all('sample-data' not in record.getMessage() for record in caplog.records)
    assert source.get_content_length() == len(payload)


def test_stream_is_closed_if_checksum_read_fails():
    class FailingStream(io.BytesIO):
        def read(self, size=-1):
            raise OSError('read failed')

    stream = FailingStream(b'content')
    source = UploadSourceStream(lambda: stream)
    with pytest.raises(OSError, match='read failed'):
        source.get_content_sha1()
    assert stream.closed


@pytest.mark.parametrize(
    'offset,length,expected', [(0, None, b'abc'), (3, None, b''), (3, 0, b''), (1, 2, b'bc')]
)
def test_file_range_accepts_valid_boundaries(tmp_path, offset, length, expected):
    path = tmp_path / 'data.bin'
    path.write_bytes(b'abc')
    source = UploadSourceLocalFileRange(path, offset=offset, length=length)
    assert source.get_content_length() == len(expected)
    with source.open() as stream:
        assert stream.read() == expected
