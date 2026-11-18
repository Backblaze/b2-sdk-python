######################################################################
#
# File: test/unit/stream/test_regression_streams.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for stream and upload-source classes.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import hashlib
import io

import pytest

from b2sdk._internal.stream.chained import ChainedStream
from b2sdk._internal.stream.range import RangeOfInputStream
from b2sdk._internal.transfer.outbound.upload_source import (
    UploadSourceLocalFileRange,
    UploadSourceStream,
    UploadSourceStreamRange,
)
from b2sdk._internal.utils import hex_sha1_of_unlimited_stream
from test.known_defect import known_defect


class ShortReadStream(io.RawIOBase):
    """A pipe-like stream that never returns more than `chunk` bytes per read, before the real end."""

    def __init__(self, data: bytes, chunk: int):
        self._buf = io.BytesIO(data)
        self._chunk = chunk

    def readable(self):
        return True

    def read(self, size=-1):
        if size is None or size < 0 or size > self._chunk:
            size = self._chunk
        return self._buf.read(size)


class ClosableTracker(io.BytesIO):
    def close(self):
        self.was_closed = True
        super().close()


class TestHashingShortReads:
    @pytest.mark.parametrize('chunk', [1, 7, 100])
    @known_defect(
        'C16',
        'hex_sha1_of_unlimited_stream stops at the first short read, so length and SHA1 cover only a prefix',
    )
    def test_c16_hash_and_length_cover_whole_stream_with_short_reads(self, chunk):
        """C16: a source returning fewer bytes than asked must not truncate hash/length."""
        data = bytes(range(256)) * 20
        sha1, length = hex_sha1_of_unlimited_stream(ShortReadStream(data, chunk))
        assert length == len(data), f'length {length} != {len(data)}'
        assert sha1 == hashlib.sha1(data).hexdigest(), 'SHA1 does not cover the whole stream'

    @known_defect(
        'C16',
        'UploadSourceStream length and SHA1 cover only a prefix when the stream returns short reads',
    )
    def test_c16_upload_source_stream_reports_full_length_with_short_reads(self):
        """C16: UploadSourceStream computing length/sha1 from a pipe-like stream."""
        data = b'x' * 5000
        source = UploadSourceStream(lambda: ShortReadStream(data, 13))
        assert source.get_content_length() == len(data), 'length does not cover the whole stream'
        assert (
            source.get_content_sha1() == hashlib.sha1(data).hexdigest()
        ), 'SHA1 does not cover the whole stream'


class TestRangeOfInputStream:
    @known_defect(
        'C46',
        'RangeOfInputStream.read(-1) passes -1 to the inner stream and reads past the end of the range',
    )
    def test_c46_read_minus_one_does_not_go_past_range_end(self):
        """C46: read(-1) must be limited to the range, like read() is."""
        stream = RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=3)
        assert stream.read(-1) == b'234', 'read(-1) went past the end of the range'

    def test_c46_read_none_stays_within_range(self):
        """Control for C46: read(None) is limited to the range (passes today)."""
        stream = RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=3)
        assert stream.read() == b'234', 'read() went past the end of the range'


class TestChainedStream:
    @known_defect(
        'C11',
        'ChainedStream.read() with the default size raises TypeError instead of reading everything',
    )
    def test_c11_default_read_returns_everything(self):
        """C11: read() with default size reads everything (docs), not a TypeError."""
        stream = ChainedStream([lambda: io.BytesIO(b'abc'), lambda: io.BytesIO(b'def')])
        assert stream.read() == b'abcdef', 'read() did not return all data'

    @known_defect('C11', 'ChainedStream.read(None) raises TypeError instead of reading everything')
    def test_c11_explicit_none_returns_everything(self):
        """C11: read(None) is documented as 'read everything'."""
        stream = ChainedStream([lambda: io.BytesIO(b'abc'), lambda: io.BytesIO(b'def')])
        assert stream.read(None) == b'abcdef', 'read(None) did not return all data'

    def test_c12_guard_sized_reads_cross_stream_boundaries(self):
        """C12: guard that sized reads span the chained streams and end at EOF."""
        stream = ChainedStream([lambda: io.BytesIO(b'abc'), lambda: io.BytesIO(b'def')])
        assert stream.read(4) == b'abcd', 'sized read did not cross the stream boundary'
        assert stream.read(10) == b'ef', 'second sized read returned wrong data'
        assert stream.read(10) == b'', 'read at EOF did not return empty bytes'


class TestUploadSourceStreamRange:
    @known_defect('C15', 'UploadSourceStreamRange built without a length fails on the first read')
    def test_c15_stream_range_without_length_can_be_read(self):
        """C15: docs allow omitting the stream length; open() then read must work."""
        data = b'0123456789'
        source = UploadSourceStreamRange(lambda: io.BytesIO(data), offset=4)
        with source.open() as fp:
            assert fp.read() == data[4:], 'wrong data read from the stream range'

    @known_defect('C15', 'UploadSourceStreamRange built without a length cannot report its length')
    def test_c15_stream_range_without_length_computes_length(self):
        """C15: length is computed lazily when not given."""
        source = UploadSourceStreamRange(lambda: io.BytesIO(b'0123456789'), offset=0)
        assert source.get_content_length() == 10, 'wrong computed length'

    @known_defect(
        'C14', 'UploadSourceStream does not close the stream it opened to compute the checksum'
    )
    def test_c14_stream_is_closed_after_checksum_computation(self):
        """C14: computing the checksum must close the stream it opened."""
        opened = []

        def opener():
            s = ClosableTracker(b'hello world')
            s.was_closed = False
            opened.append(s)
            return s

        UploadSourceStream(opener).get_content_sha1()
        assert opened, 'the source never opened a stream'
        assert all(s.was_closed for s in opened), 'a stream opened for the checksum was left open'


class TestUploadSourceLocalFileRange:
    @pytest.mark.parametrize('offset', [-3, 15])
    @known_defect(
        'C52',
        'UploadSourceLocalFileRange accepts a negative offset or an offset past the end of the file',
    )
    def test_c52_invalid_offset_is_rejected(self, tmp_path, offset):
        """C52: negative offset or offset past end (file is 10 bytes) must be refused."""
        path = tmp_path / 'f.bin'
        path.write_bytes(b'0123456789')
        with pytest.raises(ValueError):
            UploadSourceLocalFileRange(str(path), offset=offset)


class TestUnboundWriteIntentMemory:
    @known_defect(
        'C57', 'UnboundSourceBytes copies the chunk, using about twice the documented memory'
    )
    def test_c57_unbound_buffer_wrapping_does_not_copy_the_chunk(self):
        """C57: wrapping a bytearray chunk for upload must not allocate a second full copy."""
        import tracemalloc

        from b2sdk._internal.transfer.emerge.unbound_write_intent import UnboundSourceBytes

        size = 8 * 1024 * 1024
        chunk = bytearray(size)
        tracemalloc.start()
        try:
            before = tracemalloc.get_traced_memory()[0]
            source = UnboundSourceBytes(chunk, lambda: None)
            extra = tracemalloc.get_traced_memory()[0] - before
        finally:
            tracemalloc.stop()
        assert source.get_content_length() == size, 'wrong content length'
        assert extra < size // 2, f'wrapping allocated {extra} extra bytes for a {size} byte chunk'
