######################################################################
#
# File: test/unit/stream/test_upload_source_stream_unknown_length.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import hashlib
import io

import pytest

from b2sdk.v3 import UploadSourceStreamRange


def test_stream_range_without_length_can_be_read():
    source = UploadSourceStreamRange(lambda: io.BytesIO(b'0123456789'), offset=4)
    with source.open() as stream:
        assert stream.read() == b'456789'


@pytest.mark.parametrize('offset', [0, 4, 10, 15])
def test_unknown_length_is_calculated_for_the_remainder(offset):
    data = b'0123456789'
    expected = data[offset:]
    source = UploadSourceStreamRange(lambda: io.BytesIO(data), offset=offset)
    assert source.get_content_length() == len(expected)
    assert source.get_content_sha1() == hashlib.sha1(expected).hexdigest()
    with source.open() as stream:
        assert stream.read() == expected


def test_unknown_length_supports_sized_reads_and_relative_seek():
    source = UploadSourceStreamRange(lambda: io.BytesIO(b'0123456789'), offset=4)
    with source.open() as stream:
        assert stream.read(0) == b''
        assert stream.read(2) == b'45'
        assert stream.tell() == 2
        assert stream.seek(0) == 0
        assert stream.read(-1) == b'456789'
        assert stream.tell() == 6
        assert stream.read() == b''


def test_unknown_length_and_hash_include_all_short_reads():
    class ShortReadStream(io.BytesIO):
        def read(self, size=-1):
            return super().read(2 if size is None or size < 0 else min(size, 2))

    source = UploadSourceStreamRange(lambda: ShortReadStream(b'0123456789'), offset=4)
    assert source.get_content_sha1() == hashlib.sha1(b'456789').hexdigest()
    assert source.get_content_length() == 6


def test_explicit_length_still_bounds_the_stream():
    source = UploadSourceStreamRange(lambda: io.BytesIO(b'0123456789'), offset=4, stream_length=3)
    with source.open() as stream:
        assert stream.read() == b'456'
    assert source.get_content_length() == 3
    assert source.get_content_sha1() == hashlib.sha1(b'456').hexdigest()


@pytest.mark.apiver(3)
def test_upload_with_unknown_length_uses_only_the_selected_remainder(bucket):
    source = UploadSourceStreamRange(lambda: io.BytesIO(b'0123456789'), offset=4)
    version = bucket.upload(source, 'suffix.txt')
    assert version.size == 6
    destination = io.BytesIO()
    bucket.download_file_by_name('suffix.txt').save(destination)
    assert destination.getvalue() == b'456789'


def test_unknown_length_read_none_uses_default_inner_read():
    class NoExplicitNoneStream(io.BytesIO):
        def read(self, size=-1):
            if size is None:
                raise TypeError('read size cannot be None')
            return super().read(size)

    source = UploadSourceStreamRange(lambda: NoExplicitNoneStream(b'0123456789'), offset=4)
    with source.open() as stream:
        assert stream.read(None) == b'456789'
