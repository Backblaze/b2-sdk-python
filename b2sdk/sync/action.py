######################################################################
#
# File: b2sdk/sync/action.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod

import logging
import os
from .encryption_provider import AbstractSyncEncryptionSettingsProvider
from ..bucket import Bucket

from ..http_constants import SRC_LAST_MODIFIED_MILLIS
from ..transfer.outbound.upload_source import UploadSourceLocalFile
from .path import B2SyncPath
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

    def run(self, bucket, reporter, dry_run=False):
        """
        Main action routine.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
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
    def get_bytes(self):
        """
        Return the number of bytes to transfer for this action.

        :rtype: int
        """

    @abstractmethod
    def do_action(self, bucket, reporter):
        """
        Perform the action, returning only after the action is completed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """

    @abstractmethod
    def do_report(self, bucket, reporter):
        """
        Report the action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """


class B2UploadAction(AbstractAction):
    """
    File uploading action.
    """

    def __init__(
        self,
        local_full_path,
        relative_name,
        b2_file_name,
        mod_time_millis,
        size,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
    ):
        """
        :param str local_full_path: a local file path
        :param str relative_name: a relative file name
        :param str b2_file_name: a name of a new remote file
        :param int mod_time_millis: file modification time in milliseconds
        :param int size: a file size
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        self.local_full_path = local_full_path
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.mod_time_millis = mod_time_millis
        self.size = size
        self.encryption_settings_provider = encryption_settings_provider

    def get_bytes(self):
        """
        Return file size.

        :rtype: int
        """
        return self.size

    def do_action(self, bucket, reporter):
        """
        Perform the uploading action, returning only after the action is completed.

        :param b2sdk.v1.Bucket bucket: a Bucket object
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
        bucket.upload(
            UploadSourceLocalFile(self.local_full_path),
            self.b2_file_name,
            file_info=file_info,
            progress_listener=progress_listener,
            encryption=encryption,
        )

    def do_report(self, bucket, reporter):
        """
        Report the uploading action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        reporter.print_completion('upload ' + self.relative_name)

    def __str__(self):
        return 'b2_upload(%s, %s, %s)' % (
            self.local_full_path, self.b2_file_name, self.mod_time_millis
        )


class B2HideAction(AbstractAction):
    def __init__(self, relative_name, b2_file_name):
        """
        :param relative_name: a relative file name
        :type relative_name: str
        :param b2_file_name: a name of a remote file
        :type b2_file_name: str
        """
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name

    def get_bytes(self):
        """
        Return file size.

        :return: always zero
        :rtype: int
        """
        return 0

    def do_action(self, bucket, reporter):
        """
        Perform the hiding action, returning only after the action is completed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        bucket.hide_file(self.b2_file_name)

    def do_report(self, bucket, reporter):
        """
        Report the hiding action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        reporter.update_transfer(1, 0)
        reporter.print_completion('hide   ' + self.relative_name)

    def __str__(self):
        return 'b2_hide(%s)' % (self.b2_file_name,)


class B2DownloadAction(AbstractAction):
    def __init__(
        self,
        source_path: B2SyncPath,
        b2_file_name: str,
        local_full_path: str,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
    ):
        """
        :param b2sdk.v1.B2SyncPath source_path: the file to be downloaded
        :param str b2_file_name: b2_file_name
        :param str local_full_path: a local file path
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        self.source_path = source_path
        self.b2_file_name = b2_file_name
        self.local_full_path = local_full_path
        self.encryption_settings_provider = encryption_settings_provider

    def get_bytes(self):
        """
        Return file size.

        :rtype: int
        """
        return self.source_path.size

    def _ensure_directory_existence(self):
        parent_dir = os.path.dirname(self.local_full_path)
        if not os.path.isdir(parent_dir):
            try:
                os.makedirs(parent_dir)
            except OSError:
                pass
        if not os.path.isdir(parent_dir):
            raise Exception('could not create directory %s' % (parent_dir,))

    def do_action(self, bucket, reporter):
        """
        Perform the downloading action, returning only after the action is completed.

        :param b2sdk.v1.Bucket bucket: a Bucket object
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

        downloaded_file = bucket.download_file_by_id(
            self.source_path.selected_version.id_,
            progress_listener=progress_listener,
            encryption=encryption,
        )
        downloaded_file.save_to(download_path)

        # Move the file into place
        try:
            os.unlink(self.local_full_path)
        except OSError:
            pass
        os.rename(download_path, self.local_full_path)

    def do_report(self, bucket, reporter):
        """
        Report the downloading action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        reporter.print_completion('dnload ' + self.source_path.relative_path)

    def __str__(self):
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
        source_path: B2SyncPath,
        dest_b2_file_name,
        source_bucket: Bucket,
        destination_bucket: Bucket,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
    ):
        """
        :param str b2_file_name: a b2_file_name
        :param b2sdk.v1.B2SyncPath source_path: the file to be copied
        :param str dest_b2_file_name: a name of a destination remote file
        :param Bucket source_bucket: bucket to copy from
        :param Bucket destination_bucket: bucket to copy to
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        self.b2_file_name = b2_file_name
        self.source_path = source_path
        self.dest_b2_file_name = dest_b2_file_name
        self.encryption_settings_provider = encryption_settings_provider
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket

    def get_bytes(self):
        """
        Return file size.

        :rtype: int
        """
        return self.source_path.size

    def do_action(self, bucket, reporter):
        """
        Perform the copying action, returning only after the action is completed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
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

    def do_report(self, bucket, reporter):
        """
        Report the copying action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        reporter.print_completion('copy ' + self.source_path.relative_path)

    def __str__(self):
        return (
            'b2_copy(%s, %s, %s, %d)' % (
                self.b2_file_name, self.source_path.selected_version.id_, self.dest_b2_file_name,
                self.source_path.mod_time
            )
        )


class B2DeleteAction(AbstractAction):
    def __init__(self, relative_name, b2_file_name, file_id, note):
        """
        :param str relative_name: a relative file name
        :param str b2_file_name: a name of a remote file
        :param str file_id: a file ID
        :param str note: a deletion note
        """
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.file_id = file_id
        self.note = note

    def get_bytes(self):
        """
        Return file size.

        :return: always zero
        :rtype: int
        """
        return 0

    def do_action(self, bucket, reporter):
        """
        Perform the deleting action, returning only after the action is completed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        bucket.api.delete_file_version(self.file_id, self.b2_file_name)

    def do_report(self, bucket, reporter):
        """
        Report the deleting action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.relative_name + ' ' + self.note)

    def __str__(self):
        return 'b2_delete(%s, %s, %s)' % (self.b2_file_name, self.file_id, self.note)


class LocalDeleteAction(AbstractAction):
    def __init__(self, relative_name, full_path):
        """
        :param relative_name: a relative file name
        :type relative_name: str
        :param full_path: a full local path
        :type: str
        """
        self.relative_name = relative_name
        self.full_path = full_path

    def get_bytes(self):
        """
        Return file size.

        :return: always zero
        :rtype: int
        """
        return 0

    def do_action(self, bucket, reporter):
        """
        Perform the deleting of a local file action,
        returning only after the action is completed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        os.unlink(self.full_path)

    def do_report(self, bucket, reporter):
        """
        Report the deleting of a local file action performed.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        :param reporter: a place to report errors
        """
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.relative_name)

    def __str__(self):
        return 'local_delete(%s)' % (self.full_path)
