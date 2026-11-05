######################################################################
#
# File: test/unit/stream/test_range.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import io

import pytest

from b2sdk.v3 import RangeOfInputStream


@pytest.mark.parametrize('size', [-1, -2, None, 99])
@pytest.mark.parametrize('already_read', [0, 1, 3])
def test_read_all_stays_within_range(size, already_read):
    with RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=3) as stream:
        assert stream.read(already_read) == b'234'[:already_read]
        assert stream.read(size) == b'234'[already_read:]
        assert stream.tell() == 3
        assert stream.read(size) == b''
        assert stream.tell() == 3


def test_negative_read_of_empty_range_does_not_consume_source():
    with RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=0) as stream:
        assert stream.read(-1) == b''
        assert stream.tell() == 0
        assert stream.stream.tell() == 2


def test_zero_and_bounded_reads_preserve_position():
    with RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=3) as stream:
        assert stream.read(0) == b''
        assert stream.tell() == 0
        assert stream.read(2) == b'23'
        assert stream.tell() == 2
        assert stream.read(2) == b'4'
        assert stream.tell() == 3
