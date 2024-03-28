######################################################################
#
# File: b2sdk/_internal/stream/hashing.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
import io

from b2sdk._internal.stream.base import ReadOnlyStreamMixin
from b2sdk._internal.stream.wrapper import StreamWithLengthWrapper


class StreamWithHash(ReadOnlyStreamMixin, StreamWithLengthWrapper):
    """
    Wrap a file-like object, calculates SHA1 while reading
    and appends hash at the end.
    """

    def __init__(self, stream, stream_length=None):
        """
        :param stream: the stream to read from
        """
        self.digest = self.get_digest()
        total_length = None
        if stream_length is not None:
            total_length = stream_length + self.digest.digest_size * 2
        super().__init__(stream, length=total_length)
        self.hash = None
        self.hash_read = 0

    def seek(self, pos, whence=0):
        """
        Seek to a given position in the stream.

        :param int pos: position in the stream
        """
        if pos != 0 or whence != 0:
            raise io.UnsupportedOperation('Stream with hash can only be seeked to beginning')
        self.digest = self.get_digest()
        self.hash = None
        self.hash_read = 0
        return super().seek(0)

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: read data
        :rtype: bytes|None
        """
        data = b''
        if self.hash is None:
            data = super().read(size)
            # Update hash
            self.digest.update(data)

            # Check for end of stream
            if size is None or len(data) < size:
                self.hash = self.digest.hexdigest()
                if size is not None:
                    size -= len(data)

        if self.hash is not None:
            # The end of stream was reached, return hash now
            size = size or len(self.hash)
            data += str.encode(self.hash[self.hash_read:self.hash_read + size])
            self.hash_read += size
        return data

    @classmethod
    def get_digest(cls):
        return hashlib.sha1()
