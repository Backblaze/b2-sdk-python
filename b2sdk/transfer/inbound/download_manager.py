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
from typing import Optional

from b2sdk.encryption.setting import EncryptionSetting
from b2sdk.progress import DoNothingProgressListener

from b2sdk.exception import (
    InvalidRange,
)
from b2sdk.utils import B2TraceMetaAbstract

from .downloaded_file import DownloadedFile
from .downloader.parallel import ParallelDownloader
from .downloader.simple import SimpleDownloader

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
            SimpleDownloader(
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=self.MAX_CHUNK_SIZE,
            ),
        ]

    def download_file_from_url(
        self,
        url,
        progress_listener=None,
        range_=None,
        encryption: Optional[EncryptionSetting] = None,
    ) -> DownloadedFile:
        """
        :param url: url from which the file should be downloaded
        :param progress_listener: where to notify about downloading progress
        :param range_: 2-element tuple containing data of http Range header
        :param b2sdk.v1.EncryptionSetting encryption: encryption setting (``None`` if unknown)
        """
        progress_listener = progress_listener or DoNothingProgressListener()
        with self.services.session.download_file_from_url(
            url,
            range_=range_,
            encryption=encryption,
        ) as response:
            download_version = self.services.api.download_version_factory.from_response_headers(
                response.headers
            )
            if range_ is not None:
                # 2021-05-20: unfortunately for a read of a complete object server does not return the 'Content-Range' header
                if (range_[1] - range_[0] + 1) != download_version.content_length:
                    raise InvalidRange(download_version.content_length, range_)

            return DownloadedFile(
                download_version=download_version,
                download_manager=self,
                range_=range_,
                response=response,
                encryption=encryption,
                progress_listener=progress_listener,
            )
