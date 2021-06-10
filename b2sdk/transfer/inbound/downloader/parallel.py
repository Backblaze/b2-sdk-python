######################################################################
#
# File: b2sdk/transfer/inbound/downloader/parallel.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod
from io import IOBase
from typing import Optional
import logging
import hashlib
import queue
import threading

from requests.models import Response

from .abstract import AbstractDownloader
from b2sdk.encryption.setting import EncryptionSetting
from b2sdk.file_version import DownloadVersion
from b2sdk.session import B2Session
from b2sdk.utils.range_ import Range

logger = logging.getLogger(__name__)


class ParallelDownloader(AbstractDownloader):
    # situations to consider:
    #
    # local file start                                         local file end
    # |                                                                     |
    # |                                                                     |
    # |      write range start                        write range end       |
    # |      |                                                      |       |
    # v      v                                                      v       v
    # #######################################################################
    #        |          |          |          |          |          |
    #         \        / \        / \        / \        / \        /
    #           part 1     part 2     part 3     part 4     part 5
    #         /        \ /        \ /        \ /        \ /        \
    #        |          |          |          |          |          |
    #      #######################################################################
    #      ^                                                                     ^
    #      |                                                                     |
    #      cloud file start                                         cloud file end
    #
    FINISH_HASHING_BUFFER_SIZE = 1024**2

    def __init__(self, max_streams, min_part_size, *args, **kwargs):
        """
        :param max_streams: maximum number of simultaneous streams
        :param min_part_size: minimum amount of data a single stream will retrieve, in bytes
        """
        self.max_streams = max_streams
        self.min_part_size = min_part_size
        super(ParallelDownloader, self).__init__(*args, **kwargs)

    def is_suitable(self, download_version: DownloadVersion, allow_seeking: bool):
        if not super().is_suitable(download_version, allow_seeking):
            return False
        return self._get_number_of_streams(
            download_version.content_length
        ) >= 2 and download_version.content_length >= 2 * self.min_part_size

    def _get_number_of_streams(self, content_length):
        return min(self.max_streams, content_length // self.min_part_size) or 1

    def download(
        self,
        file: IOBase,
        response: Response,
        download_version: DownloadVersion,
        session: B2Session,
        encryption: Optional[EncryptionSetting] = None,
    ):
        """
        Download a file from given url using parallel download sessions and stores it in the given download_destination.
        """
        remote_range = self._get_remote_range(response, download_version)
        actual_size = remote_range.size()
        start_file_position = file.tell()
        parts_to_download = list(
            gen_parts(
                remote_range,
                Range(start_file_position, start_file_position + actual_size - 1),
                part_count=self._get_number_of_streams(download_version.content_length),
            )
        )

        first_part = parts_to_download[0]

        hasher = hashlib.sha1()

        with WriterThread(file, max_queue_depth=len(parts_to_download) * 2) as writer:
            self._get_parts(
                response,
                session,
                writer,
                hasher,
                first_part,
                parts_to_download[1:],
                self._get_chunk_size(actual_size),
                encryption=encryption,
            )
        bytes_written = writer.total

        # At this point the hasher already consumed the data until the end of first stream.
        # Consume the rest of the file to complete the hashing process
        self._finish_hashing(first_part, file, hasher, download_version.content_length)

        return bytes_written, hasher.hexdigest()

    def _finish_hashing(self, first_part, file, hasher, content_length):
        end_of_first_part = first_part.local_range.end + 1
        file.seek(end_of_first_part)
        file_read = file.read

        last_offset = first_part.local_range.start + content_length
        current_offset = end_of_first_part
        stop = False
        while 1:
            data = file_read(self.FINISH_HASHING_BUFFER_SIZE)
            if not data:
                break
            if current_offset + len(data) >= last_offset:
                to_hash = data[:last_offset - current_offset]
                stop = True
            else:
                to_hash = data
            hasher.update(data)
            current_offset += len(to_hash)
            if stop:
                break

    def _get_parts(
        self, response, session, writer, hasher, first_part, parts_to_download, chunk_size,
        encryption
    ):
        stream = FirstPartDownloaderThread(
            response,
            hasher,
            session,
            writer,
            first_part,
            chunk_size,
            encryption=encryption,
        )
        stream.start()
        streams = [stream]

        for part in parts_to_download:
            stream = NonHashingDownloaderThread(
                response.request.url,
                session,
                writer,
                part,
                chunk_size,
                encryption=encryption,
            )
            stream.start()
            streams.append(stream)
        for stream in streams:
            stream.join()


class WriterThread(threading.Thread):
    """
    A thread responsible for keeping a queue of data chunks to write to a file-like object and for actually writing them down.
    Since a single thread is responsible for synchronization of the writes, we avoid a lot of issues between userspace and kernelspace
    that would normally require flushing buffers between the switches of the writer. That would kill performance and not synchronizing
    would cause data corruption (probably we'd end up with a file with unexpected blocks of zeros preceding the range of the writer
    that comes second and writes further into the file).

    The object of this class is also responsible for backpressure: if items are added to the queue faster than they can be written
    (see GCP VMs with standard PD storage with faster CPU and network than local storage,
    https://github.com/Backblaze/B2_Command_Line_Tool/issues/595), then ``obj.queue.put(item)`` will block, slowing down the producer.

    The recommended minimum value of ``max_queue_depth`` is equal to the amount of producer threads, so that if all producers
    submit a part at the exact same time (right after network issue, for example, or just after starting the read), they can continue
    their work without blocking. The writer should be able to store at least one data chunk before a new one is retrieved, but
    it is not guaranteed.

    Therefore, the recommended value of ``max_queue_depth`` is higher - a double of the amount of producers, so that spikes on either
    end (many producers submit at the same time / consumer has a latency spike) can be accommodated without sacrificing performance.

    Please note that a size of the chunk and the queue depth impact the memory footprint. In a default setting as of writing this,
    that might be 10 downloads, 8 producers, 1MB buffers, 2 buffers each = 8*2*10 = 160 MB (+ python buffers, operating system etc).
    """

    def __init__(self, file, max_queue_depth):
        self.file = file
        self.queue = queue.Queue(max_queue_depth)
        self.total = 0
        super(WriterThread, self).__init__()

    def run(self):
        file = self.file
        queue_get = self.queue.get
        while 1:
            shutdown, offset, data = queue_get()
            if shutdown:
                break
            file.seek(offset)
            file.write(data)
            self.total += len(data)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.queue.put((True, None, None))
        self.join()


class AbstractDownloaderThread(threading.Thread):
    def __init__(
        self,
        session,
        writer,
        part_to_download,
        chunk_size,
        encryption: Optional[EncryptionSetting] = None,
    ):
        """
        :param session: raw_api wrapper
        :param writer: where to write data
        :param part_to_download: PartToDownload object
        :param chunk_size: internal buffer size to use for writing and hashing
        """
        self.session = session
        self.writer = writer
        self.part_to_download = part_to_download
        self.chunk_size = chunk_size
        self.encryption = encryption
        super(AbstractDownloaderThread, self).__init__()

    @abstractmethod
    def run(self):
        pass


class FirstPartDownloaderThread(AbstractDownloaderThread):
    def __init__(self, response, hasher, *args, **kwargs):
        """
        :param response: response of the original GET call
        :param hasher: hasher object to feed to as the stream is written
        """
        self.response = response
        self.hasher = hasher
        super(FirstPartDownloaderThread, self).__init__(*args, **kwargs)

    def run(self):
        writer_queue_put = self.writer.queue.put
        hasher_update = self.hasher.update
        first_offset = self.part_to_download.local_range.start
        last_offset = self.part_to_download.local_range.end + 1
        actual_part_size = self.part_to_download.local_range.size()
        starting_cloud_range = self.part_to_download.cloud_range

        bytes_read = 0
        stop = False
        for data in self.response.iter_content(chunk_size=self.chunk_size):
            if first_offset + bytes_read + len(data) >= last_offset:
                to_write = data[:last_offset - bytes_read]
                stop = True
            else:
                to_write = data
            writer_queue_put((False, first_offset + bytes_read, to_write))
            hasher_update(to_write)
            bytes_read += len(to_write)
            if stop:
                break

        # since we got everything we need from original response, close the socket and free the buffer
        # to avoid a timeout exception during hashing and other trouble
        self.response.close()

        url = self.response.request.url
        tries_left = 5 - 1  # this is hardcoded because we are going to replace the entire retry interface soon, so we'll avoid deprecation here and keep it private
        while tries_left and bytes_read < actual_part_size:
            cloud_range = starting_cloud_range.subrange(
                bytes_read, actual_part_size - 1
            )  # first attempt was for the whole file, but retries are bound correctly
            logger.debug(
                'download attempts remaining: %i, bytes read already: %i. Getting range %s now.',
                tries_left, bytes_read, cloud_range
            )
            with self.session.download_file_from_url(
                url,
                cloud_range.as_tuple(),
                encryption=self.encryption,
            ) as response:
                for to_write in response.iter_content(chunk_size=self.chunk_size):
                    writer_queue_put((False, first_offset + bytes_read, to_write))
                    hasher_update(to_write)
                    bytes_read += len(to_write)
            tries_left -= 1


class NonHashingDownloaderThread(AbstractDownloaderThread):
    def __init__(self, url, *args, **kwargs):
        """
        :param url: url of the target file
        """
        self.url = url
        super(NonHashingDownloaderThread, self).__init__(*args, **kwargs)

    def run(self):
        writer_queue_put = self.writer.queue.put
        start_range = self.part_to_download.local_range.start
        actual_part_size = self.part_to_download.local_range.size()
        bytes_read = 0

        starting_cloud_range = self.part_to_download.cloud_range

        retries_left = 5  # this is hardcoded because we are going to replace the entire retry interface soon, so we'll avoid deprecation here and keep it private
        while retries_left and bytes_read < actual_part_size:
            cloud_range = starting_cloud_range.subrange(bytes_read, actual_part_size - 1)
            logger.debug(
                'download attempts remaining: %i, bytes read already: %i. Getting range %s now.',
                retries_left, bytes_read, cloud_range
            )
            with self.session.download_file_from_url(
                self.url,
                cloud_range.as_tuple(),
                encryption=self.encryption,
            ) as response:
                for to_write in response.iter_content(chunk_size=self.chunk_size):
                    writer_queue_put((False, start_range + bytes_read, to_write))
                    bytes_read += len(to_write)
            retries_left -= 1


class PartToDownload(object):
    """
    Hold the range of a file to download, and the range of the
    local file where it should be stored.
    """

    def __init__(self, cloud_range, local_range):
        self.cloud_range = cloud_range
        self.local_range = local_range

    def __repr__(self):
        return 'PartToDownload(%s, %s)' % (self.cloud_range, self.local_range)


def gen_parts(cloud_range, local_range, part_count):
    """
    Generate a sequence of PartToDownload to download a large file as
    a collection of parts.
    """
    assert cloud_range.size() == local_range.size(), (cloud_range.size(), local_range.size())
    assert 0 < part_count <= cloud_range.size()
    offset = 0
    remaining_size = cloud_range.size()
    for i in range(part_count):
        # This rounds down, so if the parts aren't all the same size,
        # the smaller parts will come first.
        this_part_size = remaining_size // (part_count - i)
        part = PartToDownload(
            cloud_range.subrange(offset, offset + this_part_size - 1),
            local_range.subrange(offset, offset + this_part_size - 1),
        )
        logger.debug('created part to download: %s', part)
        yield part
        offset += this_part_size
        remaining_size -= this_part_size
