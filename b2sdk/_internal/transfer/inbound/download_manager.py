######################################################################
#
# File: b2sdk/_internal/transfer/inbound/download_manager.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.exception import (
    InvalidRange,
)
from b2sdk._internal.progress import AbstractProgressListener, DoNothingProgressListener
from b2sdk._internal.utils import B2TraceMetaAbstract

from ...utils.thread_pool import ThreadPoolMixin
from ..transfer_manager import TransferManager
from .downloaded_file import DownloadedFile
from .downloader.parallel import ParallelDownloader
from .downloader.simple import SimpleDownloader

logger = logging.getLogger(__name__)


class DownloadManager(TransferManager, ThreadPoolMixin, metaclass=B2TraceMetaAbstract):
    """
    Handle complex actions around downloads to free raw_api from that responsibility.
    """

    # minimum size of a download chunk
    DEFAULT_MIN_PART_SIZE = 100 * 1024 * 1024

    # block size used when downloading file. If it is set to a high value,
    # progress reporting will be jumpy, if it's too low, it impacts CPU
    MIN_CHUNK_SIZE = 8192  # ~1MB file will show ~1% progress increment
    MAX_CHUNK_SIZE = 1024**2

    PARALLEL_DOWNLOADER_CLASS = staticmethod(ParallelDownloader)
    SIMPLE_DOWNLOADER_CLASS = staticmethod(SimpleDownloader)

    def __init__(
        self,
        write_buffer_size: int | None = None,
        check_hash: bool = True,
        max_download_streams_per_file: int | None = None,
        **kwargs
    ):
        """
        Initialize the DownloadManager using the given services object.
        """

        super().__init__(**kwargs)
        self.strategies = [
            self.PARALLEL_DOWNLOADER_CLASS(
                min_part_size=self.DEFAULT_MIN_PART_SIZE,
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=max(self.MAX_CHUNK_SIZE, write_buffer_size or 0),
                align_factor=write_buffer_size,
                thread_pool=self._thread_pool,
                check_hash=check_hash,
                max_streams=max_download_streams_per_file,
            ),
            self.SIMPLE_DOWNLOADER_CLASS(
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=max(self.MAX_CHUNK_SIZE, write_buffer_size or 0),
                align_factor=write_buffer_size,
                thread_pool=self._thread_pool,
                check_hash=check_hash,
            ),
        ]
        self.write_buffer_size = write_buffer_size
        self.check_hash = check_hash

    def download_file_from_url(
        self,
        url: str,
        progress_listener: AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
    ) -> DownloadedFile:
        """
        Download file by URL.

        :param url: url from which the file should be downloaded
        :param progress_listener: where to notify about downloading progress
        :param range_: 2-element tuple containing data of http Range header
        :param b2sdk.v2.EncryptionSetting encryption: encryption setting (``None`` if unknown)
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
                write_buffer_size=self.write_buffer_size,
                check_hash=self.check_hash,
            )
