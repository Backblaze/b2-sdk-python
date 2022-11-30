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
from typing import Iterator, Optional, Union

from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.transfer.outbound.upload_source import AbstractUploadSource


class Wrapper(io.BytesIO):
    """
    Wrapper for BytesIO that knows when it has ended.

    When created, it tries to put itself into a waiting queue,
    when reading is finished from it, it pops itself.
    """

    def __init__(
        self,
        data: Union[bytes, bytearray],
        queue_for_done: queue.Queue,
        timeout_seconds: float,
    ):
        super().__init__(data)

        self.queue = queue_for_done
        self.length = len(data)
        self.timeout_seconds = timeout_seconds
        self.already_done = False

        # This will block whenever new item is added. This way we restrict amount of chunks
        # streamed from the source and amount of memory used. Note that if given chunk is not cleared
        # in timeout_seconds, we abort the operation.
        # Note that the element has no meaning, it's just something to take the space in the queue
        # and limit the reading processes.
        try:
            self.queue.put(self, timeout=self.timeout_seconds)
        except queue.Full:
            pass

    def read(self, size: Optional[int] = None) -> bytes:
        result = super().read(size)

        # Marking end of reading process when we got to the end of data as well as no more data was received.
        # Note that the process this is going through ensures that the data is read exactly once.
        # SHA1 is already calculated and planner shouldn't be using `_get_emerge_parts` on iterator that
        # runs this operation.
        is_done = self.tell() == self.length and len(result) == 0
        if is_done and not self.already_done:
            self.already_done = True
            # Pull one element from the queue of waiting elements.
            # Note that it doesn't matter which element we pull.
            # Each of them is just a placeholder that limits amount of memory used.
            try:
                self.queue.get(timeout=self.timeout_seconds)
            except queue.Empty:
                pass

        return result


class UnboundSourceBytes(AbstractUploadSource):
    def __init__(
        self,
        bytes_data: bytearray,
        buffer_queue: queue.Queue,
        timeout_seconds: float,
    ):
        self.bytes_data = bytes_data
        self.stream = Wrapper(self.bytes_data, buffer_queue, timeout_seconds)
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
            # In very small buffer sizes and large read sizes we could
            # land with multiple buffers read at once. This should happen
            # only in tests.
            self.trim_to_leftovers()

            # Download data up to the buffer_size_bytes.
            while len(self.buffer) < self.buffer_size_bytes:
                data = self.read_only_source.read(self.read_size)
                if len(data) == 0:
                    datastream_done = True
                    break

                self.buffer += data
                self.trim_to_leftovers()

            if len(self.buffer) == 0:
                break

            # Create an upload source bytes from it. Note that this
            # will block in case we have no more free slots.
            source = UnboundSourceBytes(
                self.buffer,
                self.buffer_limit_queue,
                self.queue_timeout_seconds,
            )

            intent = WriteIntent(source, destination_offset=offset)
            yield intent

            # Move further.
            offset += len(self.buffer)
            self.rotate_leftovers()

    def trim_to_leftovers(self) -> None:
        if len(self.buffer) <= self.buffer_size_bytes:
            return
        remainder = len(self.buffer) - self.buffer_size_bytes
        self.leftovers_buffer += self.buffer[-remainder:]
        self.buffer = self.buffer[:-remainder]

    def rotate_leftovers(self) -> None:
        self.buffer = self.leftovers_buffer
        self.leftovers_buffer = bytearray()
