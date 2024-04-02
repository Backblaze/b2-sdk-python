######################################################################
#
# File: b2sdk/_internal/stream/progress.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal.stream.wrapper import StreamWrapper


class AbstractStreamWithProgress(StreamWrapper):
    """
    Wrap a file-like object and updates a ProgressListener
    as data is read / written.
    In the abstract class, read and write methods do not update
    the progress - child classes shall do it.
    """

    def __init__(self, stream, progress_listener, offset=0):
        """

        :param stream: the stream to read from or write to
        :param b2sdk.v2.AbstractProgressListener progress_listener: the listener that we tell about progress
        :param int offset: the starting byte offset in the file
        """
        super().__init__(stream)
        assert progress_listener is not None
        self.progress_listener = progress_listener
        self.bytes_completed = 0
        self.offset = offset

    def _progress_update(self, delta):
        self.bytes_completed += delta
        self.progress_listener.bytes_completed(self.bytes_completed + self.offset)

    def __str__(self):
        return str(self.stream)


class ReadingStreamWithProgress(AbstractStreamWithProgress):
    """
    Wrap a file-like object, updates progress while reading.
    """

    def __init__(self, *args, **kwargs):
        length = kwargs.pop('length', None)
        super().__init__(*args, **kwargs)
        self.length = length

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        """
        data = super().read(size)
        self._progress_update(len(data))
        return data

    def seek(self, pos, whence=0):
        pos = super().seek(pos, whence=whence)
        # reset progress to current stream position - assumption is that ReadingStreamWithProgress would not be used
        # for random access streams, and seek is only used to reset stream to beginning to retry file upload
        # and multipart file upload would open and use different file descriptor for each part;
        # this logic cannot be used for WritingStreamWithProgress because multipart download has to use
        # single file descriptor and synchronize writes so seeking cannot be understood there as progress reset
        # and writing  progress is always monotonic
        self.bytes_completed = pos
        return pos

    def __len__(self):
        return self.length


class WritingStreamWithProgress(AbstractStreamWithProgress):
    """
    Wrap a file-like object; updates progress while writing.
    """

    def write(self, data):
        """
        Write data to the stream.

        :param bytes data: data to write to the stream
        """
        self._progress_update(len(data))
        return super().write(data)
