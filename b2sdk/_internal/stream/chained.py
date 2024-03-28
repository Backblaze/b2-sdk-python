######################################################################
#
# File: b2sdk/_internal/stream/chained.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io
from abc import ABCMeta, abstractmethod

from b2sdk._internal.stream.base import ReadOnlyStreamMixin


class ChainedStream(ReadOnlyStreamMixin, io.IOBase):
    """ Chains multiple streams in single stream, sort of what :py:class:`itertools.chain` does for iterators.

    Cleans up buffers of underlying streams when closed.

    Can be seeked to beginning (when retrying upload, for example).
    Closes underlying streams as soon as they reaches EOF, but clears their buffers
    when the chained stream is closed for underlying streams that follow
    :py:class:`b2sdk.v2.StreamOpener` cleanup interface, for example
    :py:class:`b2sdk.v2.CachedBytesStreamOpener`
    """

    def __init__(self, stream_openers):
        """
        :param list stream_openeres: list of callables that return opened streams
        """
        stream_openers = list(stream_openers)
        if not stream_openers:
            raise ValueError('chain_links cannot be empty')
        self.stream_openers = stream_openers
        self._stream_openers_iterator = iter(self.stream_openers)
        self._current_stream = None
        self._pos = 0
        super().__init__()

    @property
    def stream(self):
        """ Return currently processed stream. """
        if self._current_stream is None:
            self._next_stream()
        return self._current_stream

    def _reset_chain(self):
        if self._current_stream is not None:
            self._current_stream.close()
            self._current_stream = None
        self._stream_openers_iterator = iter(self.stream_openers)
        self._pos = 0

    def _next_stream(self):
        next_stream_opener = next(self._stream_openers_iterator, None)
        if next_stream_opener is not None:
            if self._current_stream is not None:
                self._current_stream.close()
            self._current_stream = next_stream_opener()

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, pos, whence=0):
        """
        Resets stream to the beginning.

        :param int pos: only allowed value is ``0``
        :param int whence: only allowed value is ``0``
        """
        if pos != 0 or whence != 0:
            raise io.UnsupportedOperation('Chained stream can only be seeked to beginning')

        self._reset_chain()

        return self.tell()

    def readable(self):
        return True

    def read(self, size=None):
        """
        Read at most `size` bytes from underlying streams, or all available data, if `size` is None or negative.
        Open the streams only when their data is needed, and possibly leave them open and part-way read for further
        reading - by subsequent calls to this method.

        :param int,None size: number of bytes to read. If omitted, ``None``,
                    or negative data is read and returned until EOF from final stream is reached

        :return: data read from the stream
        """
        byte_arrays = []

        if size < 0 or size is None:
            while 1:
                current_stream = self.stream
                buff = current_stream.read()
                byte_arrays.append(buff)
                if not buff:
                    self._next_stream()
                    if self.stream is current_stream:
                        break
        else:
            remaining = size
            while 1:
                current_stream = self.stream
                buff = current_stream.read(remaining)
                byte_arrays.append(buff)
                remaining -= len(buff)
                if remaining == 0:
                    # no need to open any other streams - we're satisfied
                    break
                if not buff:
                    self._next_stream()
                    if self.stream is current_stream:
                        break

        if not byte_arrays:
            data = byte_arrays[0]
        else:
            data = b''.join(byte_arrays)
        self._pos += len(data)
        return data

    def close(self):
        if self._current_stream is not None:
            self._current_stream.close()
        for stream_opener in self.stream_openers:
            if hasattr(stream_opener, 'cleanup'):
                stream_opener.cleanup()
        super().close()


class StreamOpener(metaclass=ABCMeta):
    """ Abstract class to define stream opener with cleanup. """

    @abstractmethod
    def __call__(self):
        """ Create or open the stream to read and return.

        Can be called multiple times, but streamed data may be cached and reused.
        """

    def cleanup(self):
        """ Clean up stream opener after chained stream closes.

        Can be used for cleaning cached data that are stored in memory
        to allow resetting chained stream without getting this data more than once,
        eg. data downloaded from external source.
        """
