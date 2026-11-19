######################################################################
#
# File: test/unit/stream/test_finding_c46_range.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import io

import pytest

from b2sdk._internal.stream.range import RangeOfInputStream


def test_c46_read_minus_one_does_not_go_past_range_end():
    with RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=3) as stream:
        assert stream.read(-1) == b'234'


@pytest.mark.parametrize('size', [-1, -7])
def test_negative_read_is_bounded_after_partial_read(size):
    with RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=3) as stream:
        assert stream.read(1) == b'2'
        assert stream.read(size) == b'34'
        assert stream.tell() == 3
        assert stream.read(size) == b''


def test_negative_read_from_empty_range_is_empty():
    with RangeOfInputStream(io.BytesIO(b'0123456789'), offset=2, length=0) as stream:
        assert stream.read(-1) == b''
        assert stream.tell() == 0
