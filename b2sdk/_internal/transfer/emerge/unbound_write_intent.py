######################################################################
#
# File: b2sdk/_internal/transfer/emerge/unbound_write_intent.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
import io
import queue
from typing import Callable, Iterator

from b2sdk._internal.transfer.emerge.exception import UnboundStreamBufferTimeout
from b2sdk._internal.transfer.emerge.write_intent import WriteIntent
from b2sdk._internal.transfer.outbound.upload_source import AbstractUploadSource


class IOWrapper(io.BytesIO):
    """
    Wrapper for BytesIO that knows when it has been read in full.

    Note that this stream should go through ``emerge_unbound``, as it's the only
    one that skips ``_get_emerge_parts`` and pushes buffers to the cloud
    exactly as they come. This way we can (somewhat) rely on check whether
    reading of this wrapper returned no more data.

    It is assumed that this object is owned by a single thread at a time.
    For that reason, no additional synchronisation is provided.
    """

    def __init__(
        self,
        data: bytes | bytearray,
        release_function: Callable[[], None],
    ):
        """
        Prepares a new ``io.BytesIO`` structure that will call
        a ``release_function`` when buffer is read in full.

        ``release_function`` can be called from another thread.
        It is called exactly once, when the read is concluded
        and the resource is about to be released

        :param data: data to be provided as a stream
        :param release_function: function to be called when resource will be released
        """
        super().__init__(data)
        self.release_function = release_function

    def close(self):
        if not self.closed:
            self.release_function()
        return super().close()


class UnboundSourceBytes(AbstractUploadSource):
    """
    Upload source that deals with a chunk of unbound data.

    It ensures that the data it provides doesn't have to be iterated
    over more than once. To do that, we have ensured that both length
    and sha1 is known. Also, it should be used only with ``emerge_unbound``,
    as it's the only plan that pushes buffers directly to the cloud.
    """

    def __init__(
        self,
        bytes_data: bytearray,
        release_function: Callable[[], None],
    ):
        """
        Prepares a new ```UploadSource`` that can be used with ``WriteIntent``.

        Calculates SHA1 and length of the data.

        :param bytes_data: data that should be uploaded, IOWrapper for this data is created.
        :param release_function: function to be called when all the ``bytes_data`` is uploaded.
        """
        self.length = len(bytes_data)
        # Prepare sha1 of the chunk upfront to ensure that nothing iterates over the stream but the upload.
        self.chunk_sha1 = hashlib.sha1(bytes_data).hexdigest()
        self.stream = IOWrapper(bytes_data, release_function)

    def get_content_sha1(self):
        return self.chunk_sha1

    def open(self):
        return self.stream

    def get_content_length(self):
        return self.length


class UnboundWriteIntentGenerator:
    """
    Generator that creates new write intents as data is streamed from an external source.

    It tries to ensure that at most ``queue_size`` buffers with size ``buffer_size_bytes``
    are allocated at any given moment.
    """

    def __init__(
        self,
        read_only_source,
        buffer_size_bytes: int,
        read_size: int,
        queue_size: int,
        queue_timeout_seconds: float,
    ):
        """
        Prepares a new intent generator for a given source.

        ``queue_size`` is handled on a best-effort basis. It's possible, in rare cases, that there will be more buffers
        available at once. With current implementation that would be the case when the whole buffer was read, but on
        the very last byte the server stopped responding and a retry is issued.

        :param read_only_source: Python object that has a ``read`` method.
        :param buffer_size_bytes: Size of a single buffer that we're to download from the source and push to the cloud.
        :param read_size: Size of a single read to be performed on ``read_only_source``.
        :param queue_size: Maximal amount of buffers that will be created.
        :param queue_timeout_seconds: Iterator will wait at most this many seconds for an empty slot
                                      for a buffer. After that time it's considered an error.
        """
        assert queue_size >= 1 and read_size > 0 and buffer_size_bytes > 0 and queue_timeout_seconds > 0.0

        self.read_only_source = read_only_source
        self.read_size = read_size

        self.buffer_size_bytes = buffer_size_bytes
        self.buffer_limit_queue = queue.Queue(maxsize=queue_size)
        self.queue_timeout_seconds = queue_timeout_seconds

        self.buffer = bytearray()
        self.leftovers_buffer = bytearray()

    def iterator(self) -> Iterator[WriteIntent]:
        """
        Creates new ``WriteIntent`` objects as the data is pulled from the ``read_only_source``.
        """
        datastream_done = False
        offset = 0

        while not datastream_done:
            self._wait_for_free_buffer_slot()

            # In very small buffer sizes and large read sizes we could
            # land with multiple buffers read at once. This should happen
            # only in tests.
            self._trim_to_leftovers()

            while len(self.buffer) < self.buffer_size_bytes:
                data = self.read_only_source.read(self.read_size)
                if len(data) == 0:
                    datastream_done = True
                    break

                self.buffer += data
                self._trim_to_leftovers()

            # If we've just started a new buffer and got an empty read on it,
            # we have no data to send and the process is finished.
            if len(self.buffer) == 0:
                self._release_buffer()
                break

            source = UnboundSourceBytes(self.buffer, self._release_buffer)
            intent = WriteIntent(source, destination_offset=offset)
            yield intent

            offset += len(self.buffer)
            self._rotate_leftovers()

        # If we didn't stream anything, we should still provide
        # at least an empty WriteIntent, so that the file will be created.
        if offset == 0:
            source = UnboundSourceBytes(bytearray(), release_function=lambda: None)
            yield WriteIntent(source, destination_offset=offset)

    def _trim_to_leftovers(self) -> None:
        if len(self.buffer) <= self.buffer_size_bytes:
            return
        remainder = len(self.buffer) - self.buffer_size_bytes
        buffer_view = memoryview(self.buffer)
        self.leftovers_buffer += buffer_view[-remainder:]
        # This conversion has little to no implication on performance.
        self.buffer = bytearray(buffer_view[:-remainder])

    def _rotate_leftovers(self) -> None:
        self.buffer = self.leftovers_buffer
        self.leftovers_buffer = bytearray()

    def _wait_for_free_buffer_slot(self) -> None:
        # Inserted item is only a placeholder. If we fail to insert it in given time, it means
        # that system is unable to process data quickly enough. By default, this timeout is around
        # a really large value (counted in minutes, not seconds) to indicate weird behaviour.
        try:
            self.buffer_limit_queue.put(1, timeout=self.queue_timeout_seconds)
        except queue.Full:
            raise UnboundStreamBufferTimeout()

    def _release_buffer(self) -> None:
        # Pull one element from the queue of waiting elements.
        # Note that it doesn't matter which element we pull.
        # Each of them is just a placeholder. Since we know that we've put them there,
        # there is no need to actually wait. The queue should contain at least one element if we got here.
        try:
            self.buffer_limit_queue.get_nowait()
        except queue.Empty as error:  # pragma: nocover
            raise RuntimeError('Buffer pulled twice from the queue.') from error
