######################################################################
#
# File: b2sdk/_internal/stream/range.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io

from b2sdk._internal.stream.base import ReadOnlyStreamMixin
from b2sdk._internal.stream.wrapper import StreamWithLengthWrapper


class RangeOfInputStream(ReadOnlyStreamMixin, StreamWithLengthWrapper):
    """
    Wrap a file-like object (read only) and read the selected
    range of the file.
    """

    def __init__(self, stream, offset, length):
        """
        :param stream: a seekable stream
        :param int offset: offset in the stream
        :param int length: max number of bytes to read
        """
        super().__init__(stream, length)
        self.offset = offset
        self.relative_pos = 0
        self.stream.seek(self.offset)

    def seek(self, pos, whence=0):
        """
        Seek to a given position in the stream.

        :param int pos: position in the stream relative to steam offset
        :return: new position relative to stream offset
        :rtype: int
        """
        if whence != 0:
            raise io.UnsupportedOperation('only SEEK_SET is supported')
        abs_pos = super().seek(self.offset + pos)
        self.relative_pos = abs_pos - self.offset
        return self.tell()

    def tell(self):
        """
        Return current stream position relative to offset.

        :rtype: int
        """
        return self.relative_pos

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        :rtype: bytes
        """
        remaining = max(0, self.length - self.relative_pos)
        if size is None:
            to_read = remaining
        else:
            to_read = min(size, remaining)
        data = self.stream.read(to_read)
        self.relative_pos += len(data)
        return data

    def close(self):
        super().close()
        # TODO: change the use cases of this class to close the file objects passed to it, instead of having
        # RangeOfInputStream close it's members upon garbage collection
        self.stream.close()


def wrap_with_range(stream, stream_length, range_offset, range_length):
    if range_offset == 0 and range_length == stream_length:
        return stream
    return RangeOfInputStream(stream, range_offset, range_length)
