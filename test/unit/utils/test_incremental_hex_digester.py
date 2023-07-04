######################################################################
#
# File: test/unit/utils/test_incremental_hex_digester.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
import io
from test.unit.test_base import TestBase

from b2sdk.utils import (
    IncrementalHexDigester,
    Sha1HexDigest,
)


class TestIncrementalHexDigester(TestBase):
    BLOCK_SIZE = 4

    def _get_sha1(self, input_data: bytes) -> Sha1HexDigest:
        return Sha1HexDigest(hashlib.sha1(input_data).hexdigest())

    def _get_digester(self, stream: io.IOBase) -> IncrementalHexDigester:
        return IncrementalHexDigester(stream, block_size=self.BLOCK_SIZE)

    def test_limited_read(self):
        limit = self.BLOCK_SIZE * 10
        input_data = b'1' * limit * 2
        stream = io.BytesIO(input_data)
        expected_sha1 = self._get_sha1(input_data[:limit])

        result_sha1 = self._get_digester(stream).update_from_stream(limit)

        self.assertEqual(expected_sha1, result_sha1)
        self.assertEqual(limit, stream.tell())

    def test_limited_read__stream_smaller_than_block_size(self):
        limit = self.BLOCK_SIZE * 99
        input_data = b'1' * (self.BLOCK_SIZE - 1)
        stream = io.BytesIO(input_data)
        expected_sha1 = self._get_sha1(input_data)

        result_sha1 = self._get_digester(stream).update_from_stream(limit)

        self.assertEqual(expected_sha1, result_sha1)
        self.assertEqual(len(input_data), stream.tell())

    def test_unlimited_read(self):
        input_data = b'1' * self.BLOCK_SIZE * 10
        stream = io.BytesIO(input_data)
        expected_sha1 = self._get_sha1(input_data)

        result_sha1 = self._get_digester(stream).update_from_stream()

        self.assertEqual(expected_sha1, result_sha1)
        self.assertEqual(len(input_data), stream.tell())

    def test_limited_and_unlimited_read(self):
        blocks_count = 5
        limit = self.BLOCK_SIZE * 5
        input_data = b'1' * limit * blocks_count
        stream = io.BytesIO(input_data)

        digester = self._get_digester(stream)

        for idx in range(blocks_count - 1):
            expected_sha1_part = self._get_sha1(input_data[:limit * (idx + 1)])
            result_sha1_part = digester.update_from_stream(limit)
            self.assertEqual(expected_sha1_part, result_sha1_part)

        expected_sha1_whole = self._get_sha1(input_data)
        result_sha1_whole = digester.update_from_stream()
        self.assertEqual(expected_sha1_whole, result_sha1_whole)
