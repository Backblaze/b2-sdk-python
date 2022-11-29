######################################################################
#
# File: b2sdk/stream/stdin.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import io
import sys

from typing import Optional

from b2sdk.stream.base import ReadOnlyStreamMixin
from b2sdk.stream.wrapper import StreamWrapper


class StdinStream(StreamWrapper, ReadOnlyStreamMixin):
    DEFAULT_BUFFER_SIZE_BYTES = 1024 * 1024

    def __init__(self, buffer_size_bytes: Optional[int] = None):
        """
        Open a new stream from STDIN in binary mode.

        Note: this stream closes during finalization.

        :param buffer_size_bytes: How big should be the buffer for reading data from stdin.  Pass 0 for unbuffered.
                                  Passing ``None`` will set it to a default of 1MB.

        :raises io.UnsupportedOperation: when running without terminal attached.
        """
        buffer_size = buffer_size_bytes or self.DEFAULT_BUFFER_SIZE_BYTES

        # This can happen for GUI/pythonw applications on Windows.
        if sys.stdin is None:
            raise io.UnsupportedOperation('Unable to stream from stdio without terminal/console.')

        try:
            stream = open(
                sys.stdin.fileno(),
                mode='rb',
                buffering=buffer_size,
                closefd=False,
            )
        except OSError:
            # Documentation says that it's possible that all sys.std* objects
            # could be changed to not contain buffer anymore. Also, this leaves us at the whim
            # of `-u` or `PYTHONUNBUFFERED` when it comes to buffering content of the stdin.
            stream = sys.stdin.buffer
        super().__init__(stream)

    def close(self) -> None:
        self.stream.close()
        super().close()

    def closed(self) -> bool:
        return self.stream.closed
