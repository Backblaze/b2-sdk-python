######################################################################
#
# File: b2sdk/stream/progress.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class AbstractStreamWithProgress(object):
    """
    Wrap a file-like object and updates a ProgressListener
    as data is read / written.
    In the abstract class, read and write methods do not update
    the progress - child classes shall do it.
    """

    def __init__(self, stream, progress_listener, offset=0):
        """

        :param stream: the stream to read from or write to
        :param b2sdk.v1.AbstractProgressListener progress_listener: the listener that we tell about progress
        :param int offset: the starting byte offset in the file
        """
        assert progress_listener is not None
        self.stream = stream
        self.progress_listener = progress_listener
        self.bytes_completed = 0
        self.offset = offset

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stream.__exit__(exc_type, exc_val, exc_tb)

    def seek(self, pos):
        """
        Seek to a given position in the stream.

        :param int pos: position in the stream
        """
        return self.stream.seek(pos)

    def tell(self):
        """
        Return current stream position.

        :rtype: int
        """
        return self.stream.tell()

    def flush(self):
        """
        Flush the stream.
        """
        self.stream.flush()

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        """
        if size is None:
            data = self.stream.read()
        else:
            data = self.stream.read(size)
        return data

    def write(self, data):
        """
        Write data to the stream.

        :param data: a data to write to the stream
        """
        self.stream.write(data)

    def _update(self, delta):
        self.bytes_completed += delta
        self.progress_listener.bytes_completed(self.bytes_completed + self.offset)


class ReadingStreamWithProgress(AbstractStreamWithProgress):
    """
    Wrap a file-like object, updates progress while reading.
    """

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        """
        data = super(ReadingStreamWithProgress, self).read(size)
        self._update(len(data))
        return data


class WritingStreamWithProgress(AbstractStreamWithProgress):
    """
    Wrap a file-like object; updates progress while writing.
    """

    def write(self, data):
        """
        Write data to the stream.

        :param bytes data: data to write to the stream
        """
        self._update(len(data))
        super(WritingStreamWithProgress, self).write(data)
