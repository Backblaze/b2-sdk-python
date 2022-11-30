import threading
from typing import Iterator

from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.transfer.outbound.upload_source import UploadSourceBytes

LOCK = threading.Lock()
COUNTER = 0
INDEX = 0
MAX_COUNTER_VALUE = 0
DELETE_EVENT = threading.Event()


class CountedUploadSourceBytes(UploadSourceBytes):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index = None
        global LOCK
        global COUNTER
        global INDEX
        global MAX_COUNTER_VALUE
        with LOCK:
            COUNTER += 1
            MAX_COUNTER_VALUE = max(COUNTER, MAX_COUNTER_VALUE)
            self.index = INDEX
            INDEX += 1
            print('New source fetched.', MAX_COUNTER_VALUE, self.index)

    def __del__(self):
        global LOCK
        global COUNTER
        global INDEX
        global MAX_COUNTER_VALUE
        with LOCK:
            COUNTER -= 1
            print('Source released', MAX_COUNTER_VALUE, self.index)

        DELETE_EVENT.set()


def unbound_write_intent_generator(read_only_source,
                                   buffer_size_bytes: int,
                                   read_size: int = 8192) -> Iterator[WriteIntent]:
    buffer = bytearray()
    leftovers_buffer = bytearray()
    datastream_done = False
    offset = 0

    while not datastream_done:
        if len(buffer) > buffer_size_bytes:
            remainder = len(buffer) - buffer_size_bytes
            leftovers_buffer += buffer[-remainder:]
            buffer = buffer[:-remainder]

        # Download data up to the buffer_size_bytes.
        while len(buffer) < buffer_size_bytes:
            data = read_only_source.read(read_size)

            if len(data) == 0:
                datastream_done = True
                break

            new_total_size = len(data) + len(buffer)
            # Trim the buffer if needed. Keep the reminder for later.
            if len(data) + len(buffer) > buffer_size_bytes:
                remainder = new_total_size - buffer_size_bytes
                leftovers_buffer += data[-remainder:]
                data = data[:-remainder]

            buffer += data

        print('Yielding', offset, datastream_done, 'buffer', buffer)
        if len(buffer) == 0:
            break

        import gc
        while True:
            DELETE_EVENT.wait(1.0)
            gc.collect()
            with LOCK:
                if COUNTER > 5:
                    continue
                break

        # Create an upload source bytes from it.
        source = CountedUploadSourceBytes(buffer)
        intent = WriteIntent(source, destination_offset=offset)
        yield intent

        # Move further.
        offset += len(buffer)
        buffer = leftovers_buffer
        leftovers_buffer = bytearray()
