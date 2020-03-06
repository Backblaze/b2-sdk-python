######################################################################
#
# File: b2sdk/stream/range.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class RangeOfInputStream(object):
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
        self.stream = stream
        self.offset = offset
        self.remaining = length

    def __enter__(self):
        self.stream.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stream.__exit__(exc_type, exc_val, exc_tb)

    def seek(self, pos):
        """
        Seek to a given position in the stream.

        :param int pos: position in the stream
        """
        self.stream.seek(self.offset + pos)

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        """
        if size is None:
            to_read = self.remaining
        else:
            to_read = min(size, self.remaining)
        data = self.stream.read(to_read)
        self.remaining -= len(data)
        return data
