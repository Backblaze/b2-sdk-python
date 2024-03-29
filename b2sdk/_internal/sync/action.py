######################################################################
#
# File: b2sdk/_internal/sync/action.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import contextlib
import logging
import os
from abc import ABCMeta, abstractmethod
from contextlib import suppress

from ..bucket import Bucket
from ..file_version import FileVersion
from ..http_constants import SRC_LAST_MODIFIED_MILLIS
from ..scan.path import B2Path
from ..sync.report import ProgressReport, SyncReport
from ..transfer.outbound.outbound_source import OutboundTransferSource
from ..transfer.outbound.upload_source import UploadSourceLocalFile
from ..utils.escape import escape_control_chars
from .encryption_provider import AbstractSyncEncryptionSettingsProvider
from .report import SyncFileReporter

logger = logging.getLogger(__name__)


class AbstractAction(metaclass=ABCMeta):
    """
    An action to take, such as uploading, downloading, or deleting
    a file.  Multi-threaded tasks create a sequence of Actions which
    are then run by a pool of threads.

    An action can depend on other actions completing.  An example of
    this is making sure a CreateBucketAction happens before an
    UploadFileAction.
    """

    def run(self, bucket: Bucket, reporter: ProgressReport, dry_run: bool = False):
        """
        Main action routine.

        :param bucket: a Bucket object
        :type bucket: b2sdk._internal.bucket.Bucket
        :param reporter: a place to report errors
        :param dry_run: if True, perform a dry run
        :type dry_run: bool
        """
        try:
            if not dry_run:
                self.do_action(bucket, reporter)
            self.do_report(bucket, reporter)
        except Exception as e:
            logger.exception('an exception occurred in a sync action')
            reporter.error(str(self) + ": " + repr(e) + ' ' + str(e))
            raise  # Re-throw so we can identify failed actions

    @abstractmethod
    def get_bytes(self) -> int:
        """
        Return the number of bytes to transfer for this action.
        """

    @abstractmethod
    def do_action(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Perform the action, returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """

    @abstractmethod
    def do_report(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Report the action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """


class B2UploadAction(AbstractAction):
    """
    File uploading action.
    """

    def __init__(
        self,
        local_full_path: str,
        relative_name: str,
        b2_file_name: str,
        mod_time_millis: int,
        size: int,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
    ):
        """
        :param local_full_path: a local file path
        :param relative_name: a relative file name
        :param b2_file_name: a name of a new remote file
        :param mod_time_millis: file modification time in milliseconds
        :param size: a file size
        :param encryption_settings_provider: encryption setting provider
        """
        self.local_full_path = local_full_path
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.mod_time_millis = mod_time_millis
        self.size = size
        self.encryption_settings_provider = encryption_settings_provider
        self.large_file_sha1 = None
        # TODO: Remove once we drop Python 3.7 support
        self.cached_upload_source = None

    def get_bytes(self) -> int:
        """
        Return file size.
        """
        return self.size

    @property
    # TODO: Use @functools.cached_property once we drop Python 3.7 support
    def _upload_source(self) -> UploadSourceLocalFile:
        """ Upload source if the file was to be uploaded in full """
        # NOTE: We're caching this to ensure that sha1 is not recalculated.
        if self.cached_upload_source is None:
            self.cached_upload_source = UploadSourceLocalFile(self.local_full_path)
        return self.cached_upload_source

    def get_all_sources(self) -> list[OutboundTransferSource]:
        """ Get list of sources required to complete this upload """
        return [self._upload_source]

    def do_action(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Perform the uploading action, returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        if reporter:
            progress_listener = SyncFileReporter(reporter)
        else:
            progress_listener = None
        file_info = {SRC_LAST_MODIFIED_MILLIS: str(self.mod_time_millis)}
        encryption = self.encryption_settings_provider.get_setting_for_upload(
            bucket=bucket,
            b2_file_name=self.b2_file_name,
            file_info=file_info,
            length=self.size,
        )

        sources = self.get_all_sources()
        large_file_sha1 = None

        if len(sources) > 1:
            # The upload will be incremental, calculate the large_file_sha1
            large_file_sha1 = self._upload_source.get_content_sha1()

        with contextlib.ExitStack() as exit_stack:
            if progress_listener:
                exit_stack.enter_context(progress_listener)
            bucket.concatenate(
                sources,
                self.b2_file_name,
                progress_listener=progress_listener,
                file_info=file_info,
                encryption=encryption,
                large_file_sha1=large_file_sha1,
            )

    def do_report(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Report the uploading action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        reporter.print_completion(f'upload {escape_control_chars(self.relative_name)}')

    def __str__(self) -> str:
        return f'b2_upload({self.local_full_path}, {self.b2_file_name}, {self.mod_time_millis})'


class B2IncrementalUploadAction(B2UploadAction):
    def __init__(
        self,
        local_full_path: str,
        relative_name: str,
        b2_file_name: str,
        mod_time_millis: int,
        size: int,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
        file_version: FileVersion | None = None,
        absolute_minimum_part_size: int | None = None,
    ):
        """
        :param local_full_path: a local file path
        :param relative_name: a relative file name
        :param b2_file_name: a name of a new remote file
        :param mod_time_millis: file modification time in milliseconds
        :param size: a file size
        :param encryption_settings_provider: encryption setting provider
        :param file_version: version of file currently on the server
        :param absolute_minimum_part_size: minimum file part size for large files
        """
        super().__init__(
            local_full_path, relative_name, b2_file_name, mod_time_millis, size,
            encryption_settings_provider
        )
        self.file_version = file_version
        self.absolute_minimum_part_size = absolute_minimum_part_size

    def get_all_sources(self) -> list[OutboundTransferSource]:
        return self._upload_source.get_incremental_sources(
            self.file_version, self.absolute_minimum_part_size
        )


class B2HideAction(AbstractAction):
    def __init__(self, relative_name: str, b2_file_name: str):
        """
        :param relative_name: a relative file name
        :param b2_file_name: a name of a remote file
        """
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name

    def get_bytes(self) -> int:
        """
        Return file size.

        :return: always zero
        :rtype: int
        """
        return 0

    def do_action(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Perform the hiding action, returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        bucket.hide_file(self.b2_file_name)

    def do_report(self, bucket: Bucket, reporter: SyncReport):
        """
        Report the hiding action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        reporter.update_transfer(1, 0)
        reporter.print_completion(f'hide   {escape_control_chars(self.relative_name)}')

    def __str__(self) -> str:
        return f'b2_hide({self.b2_file_name})'


class B2DownloadAction(AbstractAction):
    def __init__(
        self,
        source_path: B2Path,
        b2_file_name: str,
        local_full_path: str,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
    ):
        """
        :param source_path: the file to be downloaded
        :param b2_file_name: b2_file_name
        :param local_full_path: a local file path
        :param encryption_settings_provider: encryption setting provider
        """
        self.source_path = source_path
        self.b2_file_name = b2_file_name
        self.local_full_path = local_full_path
        self.encryption_settings_provider = encryption_settings_provider

    def get_bytes(self) -> int:
        """
        Return file size.
        """
        return self.source_path.size

    def _ensure_directory_existence(self) -> None:
        # TODO: this can fail to multiple reasons (e.g. path is a file, permissions etc).
        #   We could provide nice exceptions for it.
        parent_dir = os.path.dirname(self.local_full_path)
        if not os.path.isdir(parent_dir):
            with suppress(OSError):
                os.makedirs(parent_dir)
        if not os.path.isdir(parent_dir):
            raise Exception(f'could not create directory {parent_dir}')

    def do_action(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Perform the downloading action, returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        self._ensure_directory_existence()

        if reporter:
            progress_listener = SyncFileReporter(reporter)
        else:
            progress_listener = None

        # Download the file to a .tmp file
        download_path = self.local_full_path + '.b2.sync.tmp'

        encryption = self.encryption_settings_provider.get_setting_for_download(
            bucket=bucket,
            file_version=self.source_path.selected_version,
        )
        with contextlib.ExitStack() as exit_stack:
            if progress_listener:
                exit_stack.enter_context(progress_listener)
            downloaded_file = bucket.download_file_by_id(
                self.source_path.selected_version.id_,
                progress_listener=progress_listener,
                encryption=encryption,
            )
            downloaded_file.save_to(download_path)

        # Move the file into place
        with suppress(OSError):
            os.unlink(self.local_full_path)
        os.rename(download_path, self.local_full_path)

    def do_report(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Report the downloading action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        reporter.print_completion('dnload ' + self.source_path.relative_path)

    def __str__(self) -> str:
        return (
            'b2_download(%s, %s, %s, %d)' % (
                self.b2_file_name, self.source_path.selected_version.id_, self.local_full_path,
                self.source_path.mod_time
            )
        )


class B2CopyAction(AbstractAction):
    """
    File copying action.
    """

    def __init__(
        self,
        b2_file_name: str,
        source_path: B2Path,
        dest_b2_file_name,
        source_bucket: Bucket,
        destination_bucket: Bucket,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
    ):
        """
        :param b2_file_name: a b2_file_name
        :param source_path: the file to be copied
        :param dest_b2_file_name: a name of a destination remote file
        :param source_bucket: bucket to copy from
        :param destination_bucket: bucket to copy to
        :param encryption_settings_provider: encryption setting provider
        """
        self.b2_file_name = b2_file_name
        self.source_path = source_path
        self.dest_b2_file_name = dest_b2_file_name
        self.encryption_settings_provider = encryption_settings_provider
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket

    def get_bytes(self) -> int:
        """
        Return file size.
        """
        return self.source_path.size

    def do_action(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Perform the copying action, returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        if reporter:
            progress_listener = SyncFileReporter(reporter)
        else:
            progress_listener = None

        source_encryption = self.encryption_settings_provider.get_source_setting_for_copy(
            bucket=self.source_bucket,
            source_file_version=self.source_path.selected_version,
        )

        destination_encryption = self.encryption_settings_provider.get_destination_setting_for_copy(
            bucket=self.destination_bucket,
            source_file_version=self.source_path.selected_version,
            dest_b2_file_name=self.dest_b2_file_name,
        )

        with contextlib.ExitStack() as exit_stack:
            if progress_listener:
                exit_stack.enter_context(progress_listener)
            bucket.copy(
                self.source_path.selected_version.id_,
                self.dest_b2_file_name,
                length=self.source_path.size,
                progress_listener=progress_listener,
                destination_encryption=destination_encryption,
                source_encryption=source_encryption,
                source_file_info=self.source_path.selected_version.file_info,
                source_content_type=self.source_path.selected_version.content_type,
            )

    def do_report(self, bucket: Bucket, reporter: ProgressReport) -> None:
        """
        Report the copying action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        reporter.print_completion('copy ' + self.source_path.relative_path)

    def __str__(self) -> str:
        return (
            'b2_copy(%s, %s, %s, %d)' % (
                self.b2_file_name, self.source_path.selected_version.id_, self.dest_b2_file_name,
                self.source_path.mod_time
            )
        )


class B2DeleteAction(AbstractAction):
    def __init__(self, relative_name: str, b2_file_name: str, file_id: str, note: str):
        """
        :param relative_name: a relative file name
        :param b2_file_name: a name of a remote file
        :param file_id: a file ID
        :param note: a deletion note
        """
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.file_id = file_id
        self.note = note

    def get_bytes(self) -> int:
        """
        Return file size.

        :return: always zero
        """
        return 0

    def do_action(self, bucket: Bucket, reporter: ProgressReport):
        """
        Perform the deleting action, returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        bucket.api.delete_file_version(self.file_id, self.b2_file_name)

    def do_report(self, bucket: Bucket, reporter: SyncReport):
        """
        Report the deleting action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        reporter.update_transfer(1, 0)
        reporter.print_completion(f"delete {escape_control_chars(self.relative_name)} {self.note}")

    def __str__(self) -> str:
        return f'b2_delete({self.b2_file_name}, {self.file_id}, {self.note})'


class LocalDeleteAction(AbstractAction):
    def __init__(self, relative_name: str, full_path: str):
        """
        :param relative_name: a relative file name
        :param full_path: a full local path
        """
        self.relative_name = relative_name
        self.full_path = full_path

    def get_bytes(self) -> int:
        """
        Return file size.

        :return: always zero
        """
        return 0

    def do_action(self, bucket: Bucket, reporter: ProgressReport):
        """
        Perform the deleting of a local file action,
        returning only after the action is completed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        os.unlink(self.full_path)

    def do_report(self, bucket: Bucket, reporter: SyncReport):
        """
        Report the deleting of a local file action performed.

        :param bucket: a Bucket object
        :param reporter: a place to report errors
        """
        reporter.update_transfer(1, 0)
        reporter.print_completion(f'delete {escape_control_chars(self.relative_name)}')

    def __str__(self) -> str:
        return f'local_delete({self.full_path})'
