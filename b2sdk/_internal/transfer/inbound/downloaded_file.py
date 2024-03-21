######################################################################
#
# File: b2sdk/_internal/transfer/inbound/downloaded_file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import contextlib
import io
import logging
import pathlib
import sys
from typing import TYPE_CHECKING, BinaryIO

from requests.models import Response

from b2sdk._internal.exception import (
    ChecksumMismatch,
    DestinationDirectoryDoesntAllowOperation,
    DestinationDirectoryDoesntExist,
    DestinationError,
    DestinationIsADirectory,
    DestinationParentIsNotADirectory,
    TruncatedOutput,
)
from b2sdk._internal.utils import set_file_mtime
from b2sdk._internal.utils.filesystem import _IS_WINDOWS, points_to_fifo, points_to_stdout

try:
    from typing_extensions import Literal
except ImportError:
    from typing import Literal

from ...encryption.setting import EncryptionSetting
from ...file_version import DownloadVersion
from ...progress import AbstractProgressListener
from ...stream.progress import WritingStreamWithProgress

if TYPE_CHECKING:
    from .download_manager import DownloadManager

logger = logging.getLogger(__name__)


class MtimeUpdatedFile(io.IOBase):
    """
    Helper class that facilitates updating a files mod_time after closing.

    Over the time this class has grown, and now it also adds better exception handling.

    Usage:

    .. code-block: python

       downloaded_file = bucket.download_file_by_id('b2_file_id')
       with MtimeUpdatedFile('some_local_path', mod_time_millis=downloaded_file.download_version.mod_time_millis) as file:
           downloaded_file.save(file)
       #  'some_local_path' has the mod_time set according to metadata in B2
    """

    def __init__(
        self,
        path_: str | pathlib.Path,
        mod_time_millis: int,
        mode: Literal['wb', 'wb+'] = 'wb+',
        buffering: int | None = None,
    ):
        self.path = pathlib.Path(path_) if isinstance(path_, str) else path_
        self.mode = mode
        self.buffering = buffering if buffering is not None else -1
        self.mod_time_to_set = mod_time_millis
        self.file = None

    @property
    def path_(self) -> str:
        return str(self.path)

    @path_.setter
    def path_(self, value: str) -> None:
        self.path = pathlib.Path(value)

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

    def seekable(self) -> bool:
        return self.file.seekable()

    def seek(self, offset, whence=0):
        return self.file.seek(offset, whence)

    def tell(self):
        return self.file.tell()

    def __enter__(self):
        try:
            path = self.path
            if not path.parent.exists():
                raise DestinationDirectoryDoesntExist()

            if not path.parent.is_dir():
                raise DestinationParentIsNotADirectory()

            # This ensures consistency on *nix and Windows. Windows doesn't seem to raise ``IsADirectoryError`` at all,
            # so with this we actually can differentiate between permissions errors and target being a directory.
            if path.exists() and path.is_dir():
                raise DestinationIsADirectory()
        except PermissionError as ex:
            raise DestinationDirectoryDoesntAllowOperation() from ex

        try:
            self.file = open(
                self.path,
                self.mode,
                buffering=self.buffering,
            )
        except PermissionError as ex:
            raise DestinationDirectoryDoesntAllowOperation() from ex

        self.write = self.file.write
        self.read = self.file.read
        self.mode = self.file.mode
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
        set_file_mtime(self.path_, self.mod_time_to_set)

    def __str__(self):
        return str(self.path)


class DownloadedFile:
    """
    Result of a successful download initialization. Holds information about file's metadata
    and allows to perform the download.
    """

    def __init__(
        self,
        download_version: DownloadVersion,
        download_manager: DownloadManager,
        range_: tuple[int, int] | None,
        response: Response,
        encryption: EncryptionSetting | None,
        progress_listener: AbstractProgressListener,
        write_buffer_size=None,
        check_hash=True,
    ):
        self.download_version = download_version
        self.download_manager = download_manager
        self.range_ = range_
        self.response = response
        self.encryption = encryption
        self.progress_listener = progress_listener
        self.download_strategy = None
        self.write_buffer_size = write_buffer_size
        self.check_hash = check_hash

    def _validate_download(self, bytes_read, actual_sha1):
        if self.download_version.content_encoding is not None and self.download_version.api.api_config.decode_content:
            return
        if self.range_ is None:
            if bytes_read != self.download_version.content_length:
                raise TruncatedOutput(bytes_read, self.download_version.content_length)

            if (
                self.check_hash and self.download_version.content_sha1 != 'none' and
                actual_sha1 != self.download_version.content_sha1
            ):
                raise ChecksumMismatch(
                    checksum_type='sha1',
                    expected=self.download_version.content_sha1,
                    actual=actual_sha1,
                )
        else:
            desired_length = self.range_[1] - self.range_[0] + 1
            if bytes_read != desired_length:
                raise TruncatedOutput(bytes_read, desired_length)

    def save(self, file: BinaryIO, allow_seeking: bool | None = None) -> None:
        """
        Read data from B2 cloud and write it to a file-like object

        :param file: a file-like object
        :param allow_seeking: if False, download strategies that rely on seeking to write data
                              (parallel strategies) will be discarded.
        """
        if allow_seeking is None:
            allow_seeking = file.seekable()
        elif allow_seeking and not file.seekable():
            logger.warning('File is not seekable, disabling strategies that require seeking')
            allow_seeking = False

        if allow_seeking:  # check if file allows reading from arbitrary position
            try:
                file.read(0)
            except io.UnsupportedOperation:
                logger.warning(
                    'File is seekable, but does not allow reads, disabling strategies that require seeking'
                )
                allow_seeking = False

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

    def save_to(
        self,
        path_: str | pathlib.Path,
        mode: Literal['wb', 'wb+'] | None = None,
        allow_seeking: bool | None = None,
    ) -> None:
        """
        Open a local file and write data from B2 cloud to it, also update the mod_time.

        :param path_: path to file to be opened
        :param mode: mode in which the file should be opened
        :param allow_seeking: if False, download strategies that rely on seeking to write data
                              (parallel strategies) will be discarded.
        """
        path_ = pathlib.Path(path_)
        is_stdout = points_to_stdout(path_)
        if is_stdout or points_to_fifo(path_):
            if mode not in (None, 'wb'):
                raise DestinationError(f'invalid mode requested {mode!r} for FIFO file {path_!r}')

            if is_stdout and _IS_WINDOWS:
                if self.write_buffer_size and self.write_buffer_size not in (
                    -1, io.DEFAULT_BUFFER_SIZE
                ):
                    logger.warning(
                        'Unable to set arbitrary write_buffer_size for stdout on Windows'
                    )
                context = contextlib.nullcontext(sys.stdout.buffer)
            else:
                context = open(path_, 'wb', buffering=self.write_buffer_size or -1)

            try:
                with context as file:
                    return self.save(file, allow_seeking=allow_seeking)
            finally:
                if not is_stdout:
                    set_file_mtime(path_, self.download_version.mod_time_millis)

        with MtimeUpdatedFile(
            path_,
            mod_time_millis=self.download_version.mod_time_millis,
            mode=mode or 'wb+',
            buffering=self.write_buffer_size,
        ) as file:
            return self.save(file, allow_seeking=allow_seeking)
