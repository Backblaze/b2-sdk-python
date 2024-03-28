######################################################################
#
# File: b2sdk/_internal/stream/wrapper.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io


class StreamWrapper(io.IOBase):
    """
    Wrapper for a file-like object.
    """

    def __init__(self, stream):
        """
        :param stream: the stream to read from or write to
        """
        self.stream = stream
        super().__init__()

    def seekable(self):
        return self.stream.seekable()

    def seek(self, pos, whence=0):
        """
        Seek to a given position in the stream.

        :param int pos: position in the stream
        :return: new absolute position
        :rtype: int
        """
        return self.stream.seek(pos, whence)

    def tell(self):
        """
        Return current stream position.

        :rtype: int
        """
        return self.stream.tell()

    def truncate(self, size=None):
        return self.stream.truncate(size)

    def flush(self):
        """
        Flush the stream.
        """
        self.stream.flush()

    def readable(self):
        return self.stream.readable()

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        """
        if size is not None:
            return self.stream.read(size)
        else:
            return self.stream.read()

    def writable(self):
        return self.stream.writable()

    def write(self, data):
        """
        Write data to the stream.

        :param data: a data to write to the stream
        """
        return self.stream.write(data)


class StreamWithLengthWrapper(StreamWrapper):
    """
    Wrapper for a file-like object that supports `__len__` interface
    """

    def __init__(self, stream, length=None):
        """
        :param stream: the stream to read from or write to
        :param int length: length of the stream
        """
        super().__init__(stream)
        self.length = length

    def __len__(self):
        return self.length
