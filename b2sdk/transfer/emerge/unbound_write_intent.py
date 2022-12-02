######################################################################
#
# File: b2sdk/transfer/emerge/unbound_write_intent.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import io
import queue
from typing import (
    Callable,
    Iterator,
    Optional,
    Union,
)

from b2sdk.exception import B2SimpleError
from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.transfer.outbound.upload_source import AbstractUploadSource


class UnboundStreamBufferTimeout(B2SimpleError):
    pass


class IOWrapper(io.BytesIO):
    """
    Wrapper for BytesIO that knows when it has been read in full.

    When created, it tries to put itself into a waiting queue,
    when reading is finished from it (a final, empty read is returned),
    it pops itself. Effectively, number of buffers in memory at any given
    time should be equal to size of the queue + 1 (the one that is waiting
    to be added). Note that it can vary slightly in rare cases, when
    the buffer is read in full and retried.
    """

    def __init__(
        self,
        data: Union[bytes, bytearray],
        release_function: Callable[[], None],
    ):
        super().__init__(data)

        self.length = len(data)
        self.already_done = False
        self.release_function = release_function

    def read(self, size: Optional[int] = None) -> bytes:
        result = super().read(size)

        # Marking end of reading process when we got to the end of data as well as no more data was received.
        # Note that the process this is going through ensures that the data is read exactly once.
        # SHA1 is already calculated and planner shouldn't be using `_get_emerge_parts` on iterator that
        # runs this operation.
        is_done = self.tell() == self.length and len(result) == 0
        if is_done and not self.already_done:
            self.already_done = True
            self.release_function()

        return result


class UnboundSourceBytes(AbstractUploadSource):
    def __init__(
        self,
        bytes_data: bytearray,
        release_function: Callable[[], None],
    ):
        self.bytes_data = bytes_data
        self.stream = IOWrapper(self.bytes_data, release_function)
        # Prepare sha1 of the chunk to ensure that nothing have to iterate over our data.
        self.chunk_sha1 = hashlib.sha1(self.bytes_data).hexdigest()

    def get_content_sha1(self):
        return self.chunk_sha1

    def open(self):
        return self.stream

    def get_content_length(self):
        return len(self.bytes_data)


class UnboundWriteIntentGenerator:
    DONE_MARKER = object()

    def __init__(
        self,
        read_only_source,
        buffer_size_bytes: int,
        read_size: int,
        queue_size: int,
        queue_timeout_seconds: float,
    ):
        # Note that limiting of the buffer size to a minimum of 5MB happens in the bucket.
        assert queue_size >= 1 and read_size > 0 and buffer_size_bytes > 0 and queue_timeout_seconds > 0.0

        self.read_only_source = read_only_source
        self.buffer_size_bytes = buffer_size_bytes
        self.read_size = read_size
        self.queue_timeout_seconds = queue_timeout_seconds

        # Limit on this queue puts a limit on chunks fetched in parallel.
        self.buffer_limit_queue = queue.Queue(maxsize=queue_size)

        self.buffer = bytearray()
        self.leftovers_buffer = bytearray()

    def iterator(self) -> Iterator[WriteIntent]:
        datastream_done = False
        offset = 0

        while not datastream_done:
            # Wait for an empty slot. This way we ensure that buffer
            # is downloaded only when there's a place for it.
            try:
                self.buffer_limit_queue.put(1, timeout=self.queue_timeout_seconds)
            except queue.Full:
                raise UnboundStreamBufferTimeout()

            # In very small buffer sizes and large read sizes we could
            # land with multiple buffers read at once. This should happen
            # only in tests.
            self._trim_to_leftovers()

            # Download data up to the buffer_size_bytes.
            while len(self.buffer) < self.buffer_size_bytes:
                data = self.read_only_source.read(self.read_size)
                if len(data) == 0:
                    datastream_done = True
                    break

                self.buffer += data
                self._trim_to_leftovers()

            if len(self.buffer) == 0:
                break

            source = UnboundSourceBytes(self.buffer, self._release_buffer)
            intent = WriteIntent(source, destination_offset=offset)
            yield intent

            offset += len(self.buffer)
            self._rotate_leftovers()

    def _trim_to_leftovers(self) -> None:
        if len(self.buffer) <= self.buffer_size_bytes:
            return
        remainder = len(self.buffer) - self.buffer_size_bytes
        self.leftovers_buffer += self.buffer[-remainder:]
        self.buffer = self.buffer[:-remainder]

    def _rotate_leftovers(self) -> None:
        self.buffer = self.leftovers_buffer
        self.leftovers_buffer = bytearray()

    def _release_buffer(self) -> None:
        # Pull one element from the queue of waiting elements.
        # Note that it doesn't matter which element we pull.
        # Each of them is just a placeholder. Since we know that we've put them there,
        # there is no need to actually wait. The queue should contain at least one element if we got here.
        try:
            self.buffer_limit_queue.get_nowait()
        except queue.Empty as error:
            raise RuntimeError('Buffer pulled twice from the queue.') from error
