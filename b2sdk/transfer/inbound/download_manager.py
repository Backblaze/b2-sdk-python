######################################################################
#
# File: b2sdk/transfer/inbound/download_manager.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
from b2sdk.download_dest import DownloadDestProgressWrapper
from b2sdk.progress import DoNothingProgressListener

from b2sdk.exception import (
    ChecksumMismatch,
    InvalidRange,
    TruncatedOutput,
    UnexpectedCloudBehaviour,
)
from b2sdk.raw_api import SRC_LAST_MODIFIED_MILLIS
from b2sdk.utils import B2TraceMetaAbstract

from .downloader.parallel import ParallelDownloader
from .downloader.simple import SimpleDownloader
from .file_metadata import FileMetadata

logger = logging.getLogger(__name__)


class DownloadManager(metaclass=B2TraceMetaAbstract):
    """
    Handle complex actions around downloads to free raw_api from that responsibility.
    """

    # how many chunks to break a downloaded file into
    DEFAULT_MAX_STREAMS = 8

    # minimum size of a download chunk
    DEFAULT_MIN_PART_SIZE = 100 * 1024 * 1024

    # block size used when downloading file. If it is set to a high value,
    # progress reporting will be jumpy, if it's too low, it impacts CPU
    MIN_CHUNK_SIZE = 8192  # ~1MB file will show ~1% progress increment
    MAX_CHUNK_SIZE = 1024**2

    def __init__(self, services):
        """
        Initialize the DownloadManager using the given services object.

        :param b2sdk.v1.Services services:
        """

        self.services = services
        self.strategies = [
            ParallelDownloader(
                max_streams=self.DEFAULT_MAX_STREAMS,
                min_part_size=self.DEFAULT_MIN_PART_SIZE,
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=self.MAX_CHUNK_SIZE,
            ),
            # IOTDownloader(),  # TODO: curl -s httpbin.org/get | tee /dev/stderr 2>ble | sha1sum | cut -c -40
            SimpleDownloader(
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=self.MAX_CHUNK_SIZE,
            ),
        ]

    def download_file_from_url(
        self,
        url,
        download_dest,
        progress_listener=None,
        range_=None,
    ):
        """
        :param url: url from which the file should be downloaded
        :param download_dest: where to put the file when it is downloaded
        :param progress_listener: where to notify about progress downloading
        :param range_: 2-element tuple containing data of http Range header
        """
        progress_listener = progress_listener or DoNothingProgressListener()
        download_dest = DownloadDestProgressWrapper(download_dest, progress_listener)
        with self.services.session.download_file_from_url(
            url,
            range_=range_,
        ) as response:
            metadata = FileMetadata.from_response(response)
            if range_ is not None:
                if 'Content-Range' not in response.headers:
                    raise UnexpectedCloudBehaviour('Content-Range header was expected')
                if (range_[1] - range_[0] + 1) != metadata.content_length:
                    raise InvalidRange(metadata.content_length, range_)

            mod_time_millis = int(
                metadata.file_info.get(
                    SRC_LAST_MODIFIED_MILLIS,
                    response.headers['x-bz-upload-timestamp'],
                )
            )

            with download_dest.make_file_context(
                metadata.file_id,
                metadata.file_name,
                metadata.content_length,
                metadata.content_type,
                metadata.content_sha1,
                metadata.file_info,
                mod_time_millis,
                range_=range_,
            ) as file:

                for strategy in self.strategies:
                    if strategy.is_suitable(metadata, progress_listener):
                        bytes_read, actual_sha1 = strategy.download(
                            file, response, metadata, self.services.session
                        )
                        break
                else:
                    assert False, 'no strategy suitable for download was found!'

                self._validate_download(
                    range_, bytes_read, actual_sha1, metadata
                )  # raises exceptions
                return metadata.as_info_dict()

    @classmethod
    def _validate_download(cls, range_, bytes_read, actual_sha1, metadata):
        if range_ is None:
            if bytes_read != metadata.content_length:
                raise TruncatedOutput(bytes_read, metadata.content_length)

            if metadata.content_sha1 != 'none' and actual_sha1 != metadata.content_sha1:
                raise ChecksumMismatch(
                    checksum_type='sha1',
                    expected=metadata.content_sha1,
                    actual=actual_sha1,
                )
        else:
            desired_length = range_[1] - range_[0] + 1
            if bytes_read != desired_length:
                raise TruncatedOutput(bytes_read, desired_length)
