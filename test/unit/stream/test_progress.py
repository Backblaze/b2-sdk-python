######################################################################
#
# File: test/unit/stream/test_progress.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import io
from unittest.mock import Mock

from apiver_deps import ReadingStreamWithProgress


def test_reading_stream_with_progress(tmp_path):
    stream = io.BytesIO(b"1234567890")
    progress_listener = Mock()
    with ReadingStreamWithProgress(stream, progress_listener=progress_listener) as wrapped_stream:
        assert wrapped_stream.read(1) == b"1"
        assert wrapped_stream.read(2) == b"23"
        assert wrapped_stream.read(3) == b"456"

        assert progress_listener.bytes_completed.call_count == 3
        assert wrapped_stream.bytes_completed == 6

    assert not stream.closed


def test_reading_stream_with_progress__not_closing_wrapped_stream(tmp_path):
    stream = io.BytesIO(b"1234567890")
    progress_listener = Mock()
    with ReadingStreamWithProgress(stream, progress_listener=progress_listener) as wrapped_stream:
        assert wrapped_stream.read()

    assert not stream.closed


def test_reading_stream_with_progress__closed_proxy(tmp_path):
    """
    Test that the wrapped stream is closed when the original stream is closed.

    This is important for Python 3.13+ to prevent:
    'Exception ignored in: <b2sdk._internal.stream.progress.ReadingStreamWithProgress object at 0x748cf40d5180>'
    messages.
    """
    stream = io.BytesIO(b"1234567890")
    progress_listener = Mock()
    wrapped_stream = ReadingStreamWithProgress(stream, progress_listener=progress_listener)

    assert not stream.closed
    stream.close()
    assert wrapped_stream.closed
