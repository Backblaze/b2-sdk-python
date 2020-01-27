import io

from b2sdk.stream.base import ReadOnlyMixin


class ChainedStream(ReadOnlyMixin, io.IOBase):
    def __init__(self, stream_openers):
        stream_openers = list(stream_openers)
        if len(stream_openers) == 0:
            raise ValueError('chain_links cannot be empty')
        self.stream_openers = stream_openers
        self._stream_openers_iterator = iter(self.stream_openers)
        self._current_stream = None
        self._pos = 0

    @property
    def stream(self):
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
        Seek to a given position in the stream.

        :param int pos: position in the stream
        """
        if pos != 0 or whence != 0:
            raise io.UnsupportedOperation('Chained stream can only be seeked to beginning')

        self._reset_chain()

        return self.stream.seek(pos, whence)

    def readable(self):
        return True

    def read(self, size=None):
        """
        Read data from the stream.

        :param int size: number of bytes to read
        :return: data read from the stream
        """
        byte_arrays = []
        remaining = size
        prev_stream = None
        while True:
            current_stream = self.stream
            if current_stream is prev_stream:
                break
            buff = current_stream.read(remaining)
            byte_arrays.append(buff)
            if remaining is None:
                self._next_stream()
            else:
                remaining = remaining - len(buff)
                if remaining > 0:
                    self._next_stream()
                else:
                    break
            prev_stream = current_stream

        if len(byte_arrays) == 0:
            data = byte_arrays[0]
        else:
            data = bytes().join(byte_arrays)
        self._pos += len(data)
        return data

    def close(self):
        if self._current_stream is not None:
            self._current_stream.close()
        super(ChainedStream, self).close()
