######################################################################
#
# File: b2sdk/stream/hashing.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib


class StreamWithHash(object):
    """
    Wrap a file-like object, calculates SHA1 while reading
    and appends hash at the end.
    """

    def __init__(self, stream):
        """
        :param stream: the stream to read from
        """
        self.stream = stream
        self.digest = hashlib.sha1()
        self.hash = None
        self.hash_read = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stream.__exit__(exc_type, exc_val, exc_tb)

    def seek(self, pos):
        """
        Seek to a given position in the stream.

        :param int pos: position in the stream
        """
        assert pos == 0
        self.stream.seek(0)
        self.digest = hashlib.sha1()
        self.hash = None
        self.hash_read = 0

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: read data
        :rtype: bytes|None
        """
        data = b''
        if self.hash is None:
            # Read some bytes from stream
            if size is None:
                data = self.stream.read()
            else:
                data = self.stream.read(size)

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

    def hash_size(self):
        """
        Calculate the size of a hash string.

        :rtype: int
        """
        return self.digest.digest_size * 2
