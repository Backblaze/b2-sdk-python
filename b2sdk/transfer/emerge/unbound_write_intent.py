import hashlib
import io
import queue
import threading
from typing import (
    Iterator,
    Optional,
    Union,
)

from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.transfer.outbound.upload_source import AbstractUploadSource


class Wrapper(io.BytesIO):
    """
    Wrapper for BytesIO that knows when it has ended.

    When finished, it pops
    """

    def __init__(self, data: Union[bytes, bytearray], queue_for_done: queue.Queue, timeout_seconds: float):
        super().__init__(data)

        self.queue = queue_for_done
        self.length = len(data)
        self.timeout_seconds = timeout_seconds
        self.already_done = False

        # This will block whenever new item is added.
        self.queue.put(self, timeout=self.timeout_seconds)

    def read(self, __size: Optional[int] = None) -> bytes:
        result = super().read(__size)

        is_done = self.tell() == self.length and len(result) == 0
        if is_done and not self.already_done:
            self.already_done = True
            self.queue.get(timeout=self.timeout_seconds)

        return result


class CountedUploadSourceBytes(AbstractUploadSource):
    def __init__(self, bytes_data: bytearray, pop_when_done: queue.Queue, timeout_seconds: float):
        self.bytes_data = bytes_data
        self.stream = Wrapper(self.bytes_data, pop_when_done, timeout_seconds)

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
        read_size: int = 8192,
        queue_size: int = 1,
        queue_timeout_seconds: float = 30.0,
    ):
        # Note that limiting of the buffer size to a minimum of 5MB happens in the bucket.
        assert queue_size >= 1 and read_size > 0 and buffer_size_bytes > 0

        self.read_only_source = read_only_source
        self.buffer_size_bytes = buffer_size_bytes
        self.read_size = read_size
        self.queue_timeout_seconds = queue_timeout_seconds

        # Limit on this queue puts a limit on chunks fetched in parallel.
        self.pop_when_done = queue.Queue(maxsize=queue_size)

    def iterator(self) -> Iterator[WriteIntent]:
        buffer = bytearray()
        leftovers_buffer = bytearray()
        datastream_done = False
        offset = 0

        while not datastream_done:
            if len(buffer) > self.buffer_size_bytes:
                remainder = len(buffer) - self.buffer_size_bytes
                leftovers_buffer += buffer[-remainder:]
                buffer = buffer[:-remainder]

            # Download data up to the buffer_size_bytes.
            while len(buffer) < self.buffer_size_bytes:
                data = self.read_only_source.read(self.read_size)

                if len(data) == 0:
                    datastream_done = True
                    break

                new_total_size = len(data) + len(buffer)
                # Trim the buffer if needed. Keep the reminder for later.
                if len(data) + len(buffer) > self.buffer_size_bytes:
                    remainder = new_total_size - self.buffer_size_bytes
                    leftovers_buffer += data[-remainder:]
                    data = data[:-remainder]

                buffer += data

            if len(buffer) == 0:
                break

            # Create an upload source bytes from it. Note that this can block in case we have no more free slots.
            source = CountedUploadSourceBytes(buffer, self.pop_when_done, self.queue_timeout_seconds)
            intent = WriteIntent(source, destination_offset=offset)

            yield intent

            # Move further.
            offset += len(buffer)
            buffer = leftovers_buffer
            leftovers_buffer = bytearray()
