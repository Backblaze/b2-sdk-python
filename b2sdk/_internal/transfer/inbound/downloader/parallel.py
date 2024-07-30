######################################################################
#
# File: b2sdk/_internal/transfer/inbound/downloader/parallel.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import platform
import queue
import threading
from concurrent import futures
from io import IOBase
from time import perf_counter_ns

from requests import RequestException
from requests.models import Response

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.exception import B2Error, TruncatedOutput
from b2sdk._internal.file_version import DownloadVersion
from b2sdk._internal.session import B2Session
from b2sdk._internal.utils.range_ import Range

from .abstract import AbstractDownloader
from .stats_collector import StatsCollector

logger = logging.getLogger(__name__)


class ParallelDownloader(AbstractDownloader):
    """
    Downloader using threads to download&write multiple parts of an object in parallel.

    Each part is downloaded by its own thread, while all writes are done by additional dedicated thread.
    This can increase performance even for a small file, as fetching & writing can be done in parallel.
    """
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
    SUPPORTS_DECODE_CONTENT = False

    def __init__(self, min_part_size: int, max_streams: int | None = None, **kwargs):
        """
        :param max_streams: maximum number of simultaneous streams
        :param min_part_size: minimum amount of data a single stream will retrieve, in bytes
        """
        super().__init__(**kwargs)
        self.max_streams = max_streams
        self.min_part_size = min_part_size

    def _get_number_of_streams(self, content_length: int) -> int:
        num_streams = content_length // self.min_part_size
        if self.max_streams is not None:
            num_streams = min(num_streams, self.max_streams)
        else:
            max_threadpool_workers = getattr(self._thread_pool, '_max_workers', None)
            if max_threadpool_workers is not None:
                num_streams = min(num_streams, max_threadpool_workers)
        return max(num_streams, 1)

    def download(
        self,
        file: IOBase,
        response: Response,
        download_version: DownloadVersion,
        session: B2Session,
        encryption: EncryptionSetting | None = None,
    ):
        """
        Download a file from given url using parallel download sessions and stores it in the given download_destination.
        """
        remote_range = self._get_remote_range(response, download_version)
        hasher = self._get_hasher()
        if remote_range.size() == 0:
            response.close()
            return 0, hasher.hexdigest()

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
        if self._check_hash:
            # we skip hashing if we would not check it - hasher object is actually a EmptyHasher instance
            # but we avoid here reading whole file (except for the first part) from disk again
            before_hash = perf_counter_ns()
            self._finish_hashing(first_part, file, hasher, download_version.content_length)
            after_hash = perf_counter_ns()
            logger.info(
                'download stats | %s | %s total: %.3f ms',
                file,
                'finish_hash',
                (after_hash - before_hash) / 1000000,
            )

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
        stream = self._thread_pool.submit(
            download_first_part,
            response,
            hasher,
            session,
            writer,
            first_part,
            chunk_size,
            encryption=encryption,
        )
        streams = {stream}

        for part in parts_to_download:
            stream = self._thread_pool.submit(
                download_non_first_part,
                response.request.url,
                session,
                writer,
                part,
                chunk_size,
                encryption=encryption,
            )
            streams.add(stream)

            # free-up resources & check for early failures
            try:
                streams_futures = futures.wait(
                    streams, timeout=0, return_when=futures.FIRST_COMPLETED
                )
            except futures.TimeoutError:
                pass
            else:
                try:
                    for stream in streams_futures.done:
                        stream.result()
                except Exception:
                    if platform.python_implementation() == "PyPy":
                        # Await all threads to avoid PyPy hanging bug.
                        # https://github.com/pypy/pypy/issues/4994#issuecomment-2258962665
                        futures.wait(streams_futures.not_done)
                    raise
                streams = streams_futures.not_done

        futures.wait(streams)
        for stream in streams:
            stream.result()


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
        self.stats_collector = StatsCollector(str(self.file), 'writer', 'seek')
        super().__init__()

    def run(self):
        file = self.file
        queue_get = self.queue.get
        stats_collector_read = self.stats_collector.read
        stats_collector_other = self.stats_collector.other
        stats_collector_write = self.stats_collector.write

        with self.stats_collector.total:
            while 1:
                with stats_collector_read:
                    shutdown, offset, data = queue_get()

                if shutdown:
                    break

                with stats_collector_other:
                    file.seek(offset)

                with stats_collector_write:
                    file.write(data)

                self.total += len(data)

    def __enter__(self):
        self.start()
        return self

    def queue_write(self, offset: int, data: bytes) -> None:
        self.queue.put((False, offset, data))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.queue.put((True, None, None))
        self.join()
        self.stats_collector.report()


def download_first_part(
    response: Response,
    hasher,
    session: B2Session,
    writer: WriterThread,
    part_to_download: PartToDownload,
    chunk_size: int,
    encryption: EncryptionSetting | None = None,
) -> None:
    """
    :param response: response of the original GET call
    :param hasher: hasher object to feed to as the stream is written
    :param session: B2 API session
    :param writer: thread responsible for writing downloaded data
    :param part_to_download: definition of the part to be downloaded
    :param chunk_size: size (in bytes) of read data chunks
    :param encryption: encryption mode, algorithm and key
    """
    # This function contains a loop that has heavy impact on performance.
    # It has not been broken down to several small functions due to fear of
    # performance overhead of calling a python function. Advanced performance optimization
    # techniques are in use here, for example avoiding internal python getattr calls by
    # caching function signatures in local variables. Most of this code was written in
    # times where python 2.7 (or maybe even 2.6) had to be supported, so maybe some
    # of those optimizations could be removed without affecting performance.
    #
    # Due to reports of hard to debug performance issues, this code has also been riddled
    # with performance measurements. A known issue is GCP VMs which have more network speed
    # than storage speed, but end users have different issues with network and storage.
    # Basic tools to figure out where the time is being spent is a must for long-term
    # maintainability.
    writer_queue_put = writer.queue_write
    hasher_update = hasher.update
    local_range_start = part_to_download.local_range.start
    actual_part_size = part_to_download.local_range.size()
    starting_cloud_range = part_to_download.cloud_range

    bytes_read = 0
    url = response.request.url
    max_attempts = 15  # this is hardcoded because we are going to replace the entire retry interface soon, so we'll avoid deprecation here and keep it private
    attempt = 0

    stats_collector = StatsCollector(
        response.url, f'{local_range_start}:{part_to_download.local_range.end}', 'hash'
    )
    stats_collector_read = stats_collector.read
    stats_collector_other = stats_collector.other
    stats_collector_write = stats_collector.write

    with stats_collector.total:
        attempt += 1
        logger.debug(
            'download part %s %s attempt: %i, bytes read already: %i',
            url,
            part_to_download,
            attempt,
            bytes_read,
        )
        response_iterator = response.iter_content(chunk_size=chunk_size)

        part_not_completed = True
        while part_not_completed:
            with stats_collector_read:
                try:
                    data = next(response_iterator)
                except StopIteration:
                    break
                except RequestException:
                    if attempt < max_attempts:
                        break
                    else:
                        raise

            predicted_bytes_read = bytes_read + len(data)
            if predicted_bytes_read > actual_part_size:
                to_write = data[:actual_part_size - bytes_read]
                part_not_completed = False
            else:
                to_write = data

            with stats_collector_write:
                writer_queue_put(local_range_start + bytes_read, to_write)

            with stats_collector_other:
                hasher_update(to_write)

            bytes_read += len(to_write)

        # since we got everything we need from original response, close the socket and free the buffer
        # to avoid a timeout exception during hashing and other trouble
        response.close()
        while attempt < max_attempts and bytes_read < actual_part_size:
            attempt += 1
            cloud_range = starting_cloud_range.subrange(bytes_read, actual_part_size - 1)
            logger.debug(
                'download part %s %s attempt: %i, bytes read already: %i. Getting range %s now.',
                url, part_to_download, attempt, bytes_read, cloud_range
            )
            try:
                with session.download_file_from_url(
                    url,
                    cloud_range.as_tuple(),
                    encryption=encryption,
                ) as response:
                    response_iterator = response.iter_content(chunk_size=chunk_size)

                    while True:
                        with stats_collector_read:
                            try:
                                to_write = next(response_iterator)
                            except StopIteration:
                                break

                        with stats_collector_write:
                            writer_queue_put(local_range_start + bytes_read, to_write)

                        with stats_collector_other:
                            hasher_update(to_write)

                        bytes_read += len(to_write)
            except (B2Error, RequestException) as e:
                should_retry = e.should_retry_http() if isinstance(e, B2Error) else True
                if should_retry and attempt < max_attempts:
                    logger.debug(
                        'Download of %s %s attempt %d failed with %s, retrying', url,
                        part_to_download, attempt, e
                    )
                else:
                    raise

    stats_collector.report()

    if bytes_read != actual_part_size:
        logger.error(
            "Failed to download %s %s; Downloaded %d/%d after %d attempts", url, part_to_download,
            bytes_read, actual_part_size, attempt
        )
        raise TruncatedOutput(
            bytes_read=bytes_read,
            file_size=actual_part_size,
        )
    else:
        logger.debug(
            "Successfully downloaded %s %s; Downloaded %d/%d after %d attempts", url,
            part_to_download, bytes_read, actual_part_size, attempt
        )


def download_non_first_part(
    url: str,
    session: B2Session,
    writer: WriterThread,
    part_to_download: PartToDownload,
    chunk_size: int,
    encryption: EncryptionSetting | None = None,
) -> None:
    """
    :param url: download URL
    :param session: B2 API session
    :param writer: thread responsible for writing downloaded data
    :param part_to_download: definition of the part to be downloaded
    :param chunk_size: size (in bytes) of read data chunks
    :param encryption: encryption mode, algorithm and key
    """
    writer_queue_put = writer.queue_write
    local_range_start = part_to_download.local_range.start
    actual_part_size = part_to_download.local_range.size()
    starting_cloud_range = part_to_download.cloud_range

    bytes_read = 0
    max_attempts = 15  # this is hardcoded because we are going to replace the entire retry interface soon, so we'll avoid deprecation here and keep it private
    attempt = 0

    stats_collector = StatsCollector(
        url, f'{local_range_start}:{part_to_download.local_range.end}', 'none'
    )
    stats_collector_read = stats_collector.read
    stats_collector_write = stats_collector.write

    while attempt < max_attempts and bytes_read < actual_part_size:
        attempt += 1
        cloud_range = starting_cloud_range.subrange(bytes_read, actual_part_size - 1)
        logger.debug(
            'download part %s %s attempt: %i, bytes read already: %i. Getting range %s now.', url,
            part_to_download, attempt, bytes_read, cloud_range
        )

        with stats_collector.total:
            try:
                with session.download_file_from_url(
                    url,
                    cloud_range.as_tuple(),
                    encryption=encryption,
                ) as response:
                    response_iterator = response.iter_content(chunk_size=chunk_size)

                    while True:
                        with stats_collector_read:
                            try:
                                to_write = next(response_iterator)
                            except StopIteration:
                                break

                        with stats_collector_write:
                            writer_queue_put(local_range_start + bytes_read, to_write)

                        bytes_read += len(to_write)
            except (B2Error, RequestException) as e:
                should_retry = e.should_retry_http() if isinstance(e, B2Error) else True
                if should_retry and attempt < max_attempts:
                    logger.debug(
                        'Download of %s %s attempt %d failed with %s, retrying', url,
                        part_to_download, attempt, e
                    )
                else:
                    raise

    stats_collector.report()

    if bytes_read != actual_part_size:
        logger.error(
            "Failed to download %s %s; Downloaded %d/%d after %d attempts", url, part_to_download,
            bytes_read, actual_part_size, attempt
        )
        raise TruncatedOutput(
            bytes_read=bytes_read,
            file_size=actual_part_size,
        )
    else:
        logger.debug(
            "Successfully downloaded %s %s; Downloaded %d/%d after %d attempts", url,
            part_to_download, bytes_read, actual_part_size, attempt
        )


class PartToDownload:
    """
    Hold the range of a file to download, and the range of the
    local file where it should be stored.
    """

    def __init__(self, cloud_range, local_range):
        self.cloud_range = cloud_range
        self.local_range = local_range

    def __repr__(self):
        return f'PartToDownload({self.cloud_range}, {self.local_range})'


def gen_parts(cloud_range, local_range, part_count):
    """
    Generate a sequence of PartToDownload to download a large file as
    a collection of parts.
    """
    # We don't support DECODE_CONTENT here, hence cloud&local ranges have to be the same
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
