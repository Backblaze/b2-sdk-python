######################################################################
#
# File: test/unit/v1/test_progress.py
#
# Copyright 2019, Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from io import BytesIO

from ..test_base import TestBase
from .deps import StreamWithHash, hex_sha1_of_bytes


class TestHashingStream(TestBase):
    def setUp(self):
        self.data = b'01234567'
        self.stream = StreamWithHash(BytesIO(self.data))
        self.hash = hex_sha1_of_bytes(self.data)
        self.expected = self.data + self.hash.encode()

    def test_no_argument(self):
        output = self.stream.read()
        self.assertEqual(self.expected, output)

    def test_no_argument_less(self):
        output = self.stream.read(len(self.data) - 1)
        self.assertEqual(len(output), len(self.data) - 1)
        output += self.stream.read()
        self.assertEqual(self.expected, output)

    def test_no_argument_equal(self):
        output = self.stream.read(len(self.data))
        self.assertEqual(len(output), len(self.data))
        output += self.stream.read()
        self.assertEqual(self.expected, output)

    def test_no_argument_more(self):
        output = self.stream.read(len(self.data) + 1)
        self.assertEqual(len(output), len(self.data) + 1)
        output += self.stream.read()
        self.assertEqual(self.expected, output)

    def test_one_by_one(self):
        for expected_byte in self.expected:
            self.assertEqual(bytes((expected_byte,)), self.stream.read(1))
        self.assertEqual(b'', self.stream.read(1))

    def test_large_read(self):
        output = self.stream.read(1024)
        self.assertEqual(self.expected, output)
        self.assertEqual(b'', self.stream.read(1))

    def test_seek_zero(self):
        output0 = self.stream.read()
        self.stream.seek(0)
        output1 = self.stream.read()
        self.assertEqual(output0, output1)
