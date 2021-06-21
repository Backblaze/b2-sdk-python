######################################################################
#
# File: b2sdk/transfer/inbound/downloaded_file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import io
import logging
from typing import Optional, Tuple, TYPE_CHECKING

from requests.models import Response

from ...encryption.setting import EncryptionSetting
from ...file_version import DownloadVersion
from ...progress import AbstractProgressListener
from ...stream.progress import WritingStreamWithProgress

from b2sdk.exception import (
    ChecksumMismatch,
    TruncatedOutput,
)
from b2sdk.utils import set_file_mtime

if TYPE_CHECKING:
    from .download_manager import DownloadManager

logger = logging.getLogger(__name__)


class MtimeUpdatedFile(io.IOBase):
    """
    Helper class that facilitates updating a files mod_time after closing.
    Usage:

    .. code-block: python

       downloaded_file = bucket.download_file_by_id('b2_file_id')
       with MtimeUpdatedFile('some_local_path', mod_time_millis=downloaded_file.download_version.mod_time_millis) as file:
           downloaded_file.save(file)
       #  'some_local_path' has the mod_time set according to metadata in B2
    """

    def __init__(self, path_, mod_time_millis: int, mode='wb+'):
        self.path_ = path_
        self.mode = mode
        self.mod_time_to_set = mod_time_millis
        self.file = None

    def write(self, value):
        """
        This method is overwritten (monkey-patched) in __enter__ for performance reasons
        """
        raise NotImplementedError

    def read(self, *a):
        """
        This method is overwritten (monkey-patched) in __enter__ for performance reasons
        """
        raise NotImplementedError

    def seek(self, offset, whence=0):
        return self.file.seek(offset, whence)

    def tell(self):
        return self.file.tell()

    def __enter__(self):
        self.file = open(self.path_, self.mode)
        self.write = self.file.write
        self.read = self.file.read
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
        set_file_mtime(self.path_, self.mod_time_to_set)


class DownloadedFile:
    """
    Result of a successful download initialization. Holds information about file's metadata
    and allows to perform the download.
    """

    def __init__(
        self,
        download_version: DownloadVersion,
        download_manager: 'DownloadManager',
        range_: Optional[Tuple[int, int]],
        response: Response,
        encryption: Optional[EncryptionSetting],
        progress_listener: AbstractProgressListener,
    ):
        self.download_version = download_version
        self.download_manager = download_manager
        self.range_ = range_
        self.response = response
        self.encryption = encryption
        self.progress_listener = progress_listener
        self.download_strategy = None

    def _validate_download(self, bytes_read, actual_sha1):
        if self.range_ is None:
            if bytes_read != self.download_version.content_length:
                raise TruncatedOutput(bytes_read, self.download_version.content_length)

            if self.download_version.content_sha1 != 'none' and actual_sha1 != self.download_version.content_sha1:
                raise ChecksumMismatch(
                    checksum_type='sha1',
                    expected=self.download_version.content_sha1,
                    actual=actual_sha1,
                )
        else:
            desired_length = self.range_[1] - self.range_[0] + 1
            if bytes_read != desired_length:
                raise TruncatedOutput(bytes_read, desired_length)

    def save(self, file, allow_seeking=True):
        """
        Read data from B2 cloud and write it to a file-like object

        :param file: a file-like object
        :param allow_seeking: if False, download strategies that rely on seeking to write data
                              (parallel strategies) will be discarded.
        """
        if self.progress_listener:
            file = WritingStreamWithProgress(file, self.progress_listener)
            if self.range_ is not None:
                total_bytes = self.range_[1] - self.range_[0] + 1
            else:
                total_bytes = self.download_version.content_length
            self.progress_listener.set_total_bytes(total_bytes)
        for strategy in self.download_manager.strategies:
            if strategy.is_suitable(self.download_version, allow_seeking):
                break
        else:
            raise ValueError('no strategy suitable for download was found!')
        self.download_strategy = strategy
        bytes_read, actual_sha1 = strategy.download(
            file,
            response=self.response,
            download_version=self.download_version,
            session=self.download_manager.services.session,
            encryption=self.encryption,
        )
        self._validate_download(bytes_read, actual_sha1)

    def save_to(self, path_, mode='wb+', allow_seeking=True):
        """
        Open a local file and write data from B2 cloud to it, also update the mod_time.

        :param path_: path to file to be opened
        :param mode: mode in which the file should be opened
        :param allow_seeking: if False, download strategies that rely on seeking to write data
                              (parallel strategies) will be discarded.
        """
        with MtimeUpdatedFile(
            path_, mod_time_millis=self.download_version.mod_time_millis, mode=mode
        ) as file:
            self.save(file, allow_seeking=allow_seeking)
