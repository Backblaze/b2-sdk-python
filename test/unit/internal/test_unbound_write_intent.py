######################################################################
#
# File: test/unit/internal/test_unbound_write_intent.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io
import string
from unittest.mock import MagicMock

from b2sdk.transfer.emerge.unbound_write_intent import (
    IOWrapper,
    UnboundSourceBytes,
    UnboundStreamBufferTimeout,
    UnboundWriteIntentGenerator,
)
from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.utils import hex_sha1_of_bytes

from ..test_base import TestBase


class TestIOWrapper(TestBase):
    def setUp(self) -> None:
        self.data = b'test-data'
        self.mock_fun = MagicMock()
        self.wrapper = IOWrapper(self.data, release_function=self.mock_fun)

    def test_function_called_on_close_manual(self):
        self.mock_fun.assert_not_called()

        self.wrapper.read(len(self.data))
        self.mock_fun.assert_not_called()

        self.wrapper.read(len(self.data))
        self.mock_fun.assert_not_called()

        self.wrapper.close()
        self.mock_fun.assert_called_once()

    def test_function_called_on_close_context(self):
        self.mock_fun.assert_not_called()
        with self.wrapper as w:
            w.read(len(self.data))
        self.mock_fun.assert_called_once()


class TestUnboundSourceBytes(TestBase):
    def test_data_has_length_and_sha1_calculated_without_touching_the_stream(self):
        data = bytearray(b'test-data')
        mock_fun = MagicMock()
        source = UnboundSourceBytes(data, mock_fun)

        self.assertEqual(len(data), source.get_content_length())
        self.assertEqual(hex_sha1_of_bytes(data), source.get_content_sha1())
        mock_fun.assert_not_called()


class TestUnboundWriteIntentGenerator(TestBase):
    def setUp(self) -> None:
        self.data = b'test-data'
        self.kwargs = dict(
            # From the perspective of the UnboundWriteIntentGenerator itself, the queue size
            # can be any positive integer. Bucket requires it to be at least two, so that
            # it can determine the upload method.
            queue_size=1,
            queue_timeout_seconds=0.1,
        )

    def _get_iterator(self, buffer_and_read_size: int = 1, data: bytes | None = None):
        data = data or self.data
        generator = UnboundWriteIntentGenerator(
            io.BytesIO(data),
            buffer_size_bytes=buffer_and_read_size,
            read_size=buffer_and_read_size,
            **self.kwargs
        )
        return generator.iterator()

    def _read_write_intent(self, write_intent: WriteIntent, full_read_size: int = 1) -> bytes:
        buffer_stream = write_intent.outbound_source.open()  # noqa
        read_data = buffer_stream.read(full_read_size)
        empty_data = buffer_stream.read(full_read_size)
        self.assertEqual(0, len(empty_data))
        buffer_stream.close()
        return read_data

    def test_timeout_called_when_waiting_too_long_for_empty_buffer_slot(self):
        # First buffer is delivered without issues.
        iterator = self._get_iterator()
        next(iterator)
        with self.assertRaises(UnboundStreamBufferTimeout):
            # Since we didn't read the first one, the second one is blocked.
            next(iterator)

    def test_all_data_iterated_over(self):
        # This also tests empty last buffer case.
        data_loaded = []

        for write_intent in self._get_iterator():
            read_data = self._read_write_intent(write_intent, 1)
            self.assertEqual(
                self.data[write_intent.destination_offset].to_bytes(1, 'big'),
                read_data,
            )
            data_loaded.append((read_data, write_intent.destination_offset))

        expected_data_loaded = [
            (byte.to_bytes(1, 'big'), idx) for idx, byte in enumerate(self.data)
        ]
        self.assertCountEqual(expected_data_loaded, data_loaded)

    def test_larger_buffer_size(self):
        # This also tests non-empty last buffer case.
        read_size = 4
        # Build a buffer of N reads of size read_size and one more byte.
        data = b''.join(string.printable[:read_size].encode('ascii') for _ in range(2)) + b'1'

        for write_intent in self._get_iterator(read_size, data):
            read_data = self._read_write_intent(write_intent, full_read_size=read_size)
            offset = write_intent.destination_offset
            expected_data = data[offset:offset + read_size]
            self.assertEqual(expected_data, read_data)

    def test_single_buffer_delivered(self):
        read_size = len(self.data) + 1
        iterator = self._get_iterator(read_size)

        write_intent = next(iterator)
        self._read_write_intent(write_intent, full_read_size=read_size)

        with self.assertRaises(StopIteration):
            next(iterator)
