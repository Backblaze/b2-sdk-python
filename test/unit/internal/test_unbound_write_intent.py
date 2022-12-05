######################################################################
#
# File: test/unit/internal/test_unbound_write_intent.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import io
from unittest.mock import MagicMock

from b2sdk.transfer.emerge.unbound_write_intent import (
    IOWrapper,
    UnboundSourceBytes,
    UnboundStreamBufferTimeout,
    UnboundWriteIntentGenerator,
)
from b2sdk.utils import hex_sha1_of_bytes

from .test_base import TestBase


class TestIOWrapper(TestBase):
    def setUp(self) -> None:
        self.data = b'test-data'
        self.mock_fun = MagicMock()
        self.wrapper = IOWrapper(self.data, self.mock_fun)

    def test_function_called_only_after_empty_read(self):
        self.mock_fun.assert_not_called()

        self.wrapper.read(1)
        self.mock_fun.assert_not_called()

        self.wrapper.read(len(self.data) - 1)
        self.mock_fun.assert_not_called()

        self.wrapper.seek(0)
        self.mock_fun.assert_not_called()

        self.wrapper.read(len(self.data))
        self.mock_fun.assert_not_called()

        self.wrapper.seek(0)
        self.mock_fun.assert_not_called()

        for _ in range(len(self.data)):
            self.wrapper.read(1)
            self.mock_fun.assert_not_called()

        self.assertEqual(0, len(self.wrapper.read(1)))
        self.mock_fun.assert_called_once()

    def test_function_called_exactly_once(self):
        self.wrapper.read(len(self.data))
        self.wrapper.read(1)
        self.mock_fun.assert_called_once()

        self.wrapper.seek(0)
        self.wrapper.read(len(self.data))
        self.wrapper.read(1)
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

        self.generator = UnboundWriteIntentGenerator(
            io.BytesIO(self.data),
            buffer_size_bytes=1,
            read_size=1,
            queue_size=1,
            queue_timeout_seconds=0.1,
        )
        self.iterator = self.generator.iterator()

    def test_timeout_called_when_waiting_too_long_for_empty_buffer_slot(self):
        # First buffer is delivered without issues.
        next(self.iterator)
        with self.assertRaises(UnboundStreamBufferTimeout):
            # Since we didn't read the first one, the second one is blocked.
            next(self.iterator)

    def test_all_data_iterated_over(self):
        data_loaded = []

        for write_intent in self.iterator:
            buffer_stream = write_intent.outbound_source.open()  # noqa
            read_data = buffer_stream.read(1)
            self.assertEqual(self.data[write_intent.destination_offset].to_bytes(1, 'big'), read_data)
            empty_data = buffer_stream.read(1)
            self.assertEqual(0, len(empty_data))

            data_loaded.append((read_data, write_intent.destination_offset))

        expected_data_loaded = [(byte.to_bytes(1, 'big'), idx) for idx, byte in enumerate(self.data)]
        self.assertCountEqual(expected_data_loaded, data_loaded)
