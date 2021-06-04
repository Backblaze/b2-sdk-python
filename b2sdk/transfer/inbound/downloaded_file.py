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
from typing import Optional, Tuple

from requests.models import Response

from .downloader.abstract import AbstractDownloader
from ...encryption.setting import EncryptionSetting
from ...file_version import FileVersion
from ...progress import AbstractProgressListener
from ...session import B2Session
from ...stream.progress import WritingStreamWithProgress

from b2sdk.exception import (
    ChecksumMismatch,
    TruncatedOutput,
)
from b2sdk.utils import set_file_mtime

logger = logging.getLogger(__name__)


class MtimeUpdatedFile(io.IOBase):
    """
    Helper class that facilitates updating a files mod_time after closing.
    Usage:

    .. code-block: python

       downloaded_file = bucket.download_file_by_id('b2_file_id')
       with MtimeUpdatedFile('some_local_path') as file:
           downloaded_file.save(file, file.set_mod_time)
       #  'some_local_path' has the mod_time set according to metadata in B2
    """

    def __init__(self, path_, mode='wb+'):
        self.path_ = path_
        self.mode = mode
        self.mod_time_to_set = None
        self.file = None

    def set_mod_time(self, mod_time):
        self.mod_time_to_set = mod_time

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
        if self.mod_time_to_set is not None:
            set_file_mtime(self.path_, self.mod_time_to_set)


class DownloadedFile:
    def __init__(
        self,
        file_version: FileVersion,
        strategy: AbstractDownloader,
        range_: Optional[Tuple[int, int]],
        response: Response,
        encryption: Optional[EncryptionSetting],
        session: B2Session,
        progress_listener: AbstractProgressListener,
    ):
        self.file_version = file_version
        self.strategy = strategy
        self.range_ = range_
        self.progress_listener = progress_listener
        self.response = response
        self.encryption = encryption
        self.session = session

    def _validate_download(self, bytes_read, actual_sha1):
        if self.range_ is None:
            if bytes_read != self.file_version.size:
                raise TruncatedOutput(bytes_read, self.file_version.size)

            if self.file_version.content_sha1 != 'none' and actual_sha1 != self.file_version.content_sha1:
                raise ChecksumMismatch(
                    checksum_type='sha1',
                    expected=self.file_version.content_sha1,
                    actual=actual_sha1,
                )
        else:
            desired_length = self.range_[1] - self.range_[0] + 1
            if bytes_read != desired_length:
                raise TruncatedOutput(bytes_read, desired_length)

    def save(self, file, mod_time_callback=None):
        """
        Read data from B2 cloud and write it to a file-like object
        :param file: a file-like object
        :param mod_time_callback: a callable accepting a single argument: the mod time of the downloaded file in milliseconds
        """
        if self.progress_listener:
            file = WritingStreamWithProgress(file, self.progress_listener)
            if self.range_ is not None:
                total_bytes = self.range_[1] - self.range_[0] + 1
            else:
                total_bytes = self.file_version.size
            self.progress_listener.set_total_bytes(total_bytes)
        if mod_time_callback is not None:
            mod_time_callback(self.file_version.mod_time_millis)
        bytes_read, actual_sha1 = self.strategy.download(
            file,
            self.response,
            self.file_version,
            self.session,
            encryption=self.encryption,
        )
        self._validate_download(bytes_read, actual_sha1)

    def save_to(self, path_, mode='wb+'):
        """
        Open a local file and write data from B2 cloud to it, also update the mod_time.
        """
        with MtimeUpdatedFile(path_, mode) as file:
            self.save(file, file.set_mod_time)
