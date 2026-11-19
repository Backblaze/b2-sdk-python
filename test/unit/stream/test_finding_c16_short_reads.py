######################################################################
#
# File: test/unit/stream/test_finding_c16_short_reads.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io

import pytest

from b2sdk._internal.stream.hashing import StreamWithHash
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceStream
from b2sdk._internal.utils import IncrementalHexDigester, hex_sha1_of_unlimited_stream

DATA = b'0123456789'
SHA1 = '87acec17cd9dcd20a716cc2cf67417b71c8a7016'
PREFIX_SHA1 = 'c4b5c86bd577da3d93fea7c89cba61c78b48e589'
EMPTY_SHA1 = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'


class ShortReadStream(io.BytesIO):
    """A blocking source that may return less than requested before EOF."""

    def __init__(self, data, chunk_size=3):
        super().__init__(data)
        self.chunk_size = chunk_size

    def read(self, size=-1):
        if size is None or size < 0:
            return super().read()
        return super().read(min(size, self.chunk_size))


@pytest.mark.parametrize('chunk_size', [1, 3])
def test_unlimited_digest_and_length_cover_the_whole_stream(chunk_size):
    assert hex_sha1_of_unlimited_stream(ShortReadStream(DATA, chunk_size)) == (SHA1, len(DATA))


def test_limited_digest_reads_to_the_limit_then_resumes():
    stream = ShortReadStream(DATA)
    digester = IncrementalHexDigester(stream, block_size=5)

    assert digester.update_from_stream(0) == EMPTY_SHA1
    assert stream.tell() == digester.read_bytes == 0
    assert digester.update_from_stream(4) == PREFIX_SHA1
    assert stream.tell() == digester.read_bytes == 4
    assert digester.update_from_stream() == SHA1
    assert stream.tell() == digester.read_bytes == len(DATA)


def test_upload_source_reports_the_whole_length_and_digest():
    source = UploadSourceStream(lambda: ShortReadStream(DATA))
    assert source.get_content_length() == len(DATA)
    assert source.get_content_sha1() == SHA1


@pytest.mark.parametrize('data, digest', [(DATA, SHA1), (b'', EMPTY_SHA1)])
@pytest.mark.parametrize('read_size', [4, 10, 11, 64, None, -1])
def test_hashing_stream_preserves_all_data_before_appending_digest(data, digest, read_size):
    stream = StreamWithHash(ShortReadStream(data), stream_length=len(data))
    expected = data + digest.encode('ascii')
    assert len(stream) == len(expected)

    if read_size is None:
        assert stream.read() == expected
    elif read_size < 0:
        assert stream.read(read_size) == expected
    else:
        for offset in range(0, len(expected), read_size):
            assert stream.read(read_size) == expected[offset : offset + read_size]
    assert stream.read(read_size) == b''


def test_zero_reads_do_not_consume_data_or_digest():
    stream = StreamWithHash(io.BytesIO(DATA))
    expected = DATA + SHA1.encode('ascii')
    assert stream.read(0) == b''
    assert stream.read(len(DATA) + 2) == expected[: len(DATA) + 2]
    assert stream.read(0) == b''
    assert stream.read() == expected[len(DATA) + 2 :]
    assert stream.read(0) == b''


def test_zero_read_preserves_the_closed_source_error():
    source = io.BytesIO(DATA)
    stream = StreamWithHash(source)
    source.close()
    with pytest.raises(ValueError, match='closed'):
        stream.read(0)


def test_seek_to_start_resets_the_digest_after_short_reads():
    stream = StreamWithHash(ShortReadStream(DATA))
    stream.read(4)
    stream.seek(0)
    assert stream.read() == DATA + SHA1.encode('ascii')
