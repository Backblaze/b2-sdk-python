######################################################################
#
# File: b2sdk/sync/policy.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from typing import Optional

import logging

from ..exception import DestFileNewer
from .encryption_provider import AbstractSyncEncryptionSettingsProvider, SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER
from .action import LocalDeleteAction, B2CopyAction, B2DeleteAction, B2DownloadAction, B2HideAction, B2UploadAction
from .exception import InvalidArgument
from .folder import AbstractFolder
from .path import AbstractSyncPath

ONE_DAY_IN_MS = 24 * 60 * 60 * 1000

logger = logging.getLogger(__name__)


@unique
class NewerFileSyncMode(Enum):
    """ Mode of handling files newer on destination than on source """
    SKIP = 101  #: skip syncing such file
    REPLACE = 102  #: replace the file on the destination with the (older) file on source
    RAISE_ERROR = 103  #: raise a non-transient error, failing the sync operation


@unique
class CompareVersionMode(Enum):
    """ Mode of comparing versions of files to determine what should be synced and what shouldn't """
    MODTIME = 201  #: use file modification time on source filesystem
    SIZE = 202  #: compare using file size
    NONE = 203  #: compare using file name only


class AbstractFileSyncPolicy(metaclass=ABCMeta):
    """
    Abstract policy class.
    """
    DESTINATION_PREFIX = NotImplemented
    SOURCE_PREFIX = NotImplemented

    def __init__(
        self,
        source_path: AbstractSyncPath,
        source_folder: AbstractFolder,
        dest_path: AbstractSyncPath,
        dest_folder: AbstractFolder,
        now_millis: int,
        keep_days: int,
        newer_file_mode: NewerFileSyncMode,
        compare_threshold: int,
        compare_version_mode: CompareVersionMode = CompareVersionMode.MODTIME,
        encryption_settings_provider:
        AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    ):
        """
        :param b2sdk.v1.AbstractSyncPath source_path: source file object
        :param b2sdk.v1.AbstractFolder source_folder: source folder object
        :param b2sdk.v1.AbstractSyncPath dest_path: destination file object
        :param b2sdk.v1.AbstractFolder dest_folder: destination folder object
        :param int now_millis: current time in milliseconds
        :param int keep_days: days to keep before delete
        :param b2sdk.v1.NewerFileSyncMode newer_file_mode: setting which determines handling for destination files newer than on the source
        :param int compare_threshold: when comparing with size or time for sync
        :param b2sdk.v1.COMPARE_VERSION_MODES compare_version_mode: how to compare source and destination files
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        self._source_path = source_path
        self._source_folder = source_folder
        self._dest_path = dest_path
        self._keep_days = keep_days
        self._newer_file_mode = newer_file_mode
        self._compare_version_mode = compare_version_mode
        self._compare_threshold = compare_threshold
        self._dest_folder = dest_folder
        self._now_millis = now_millis
        self._transferred = False
        self._encryption_settings_provider = encryption_settings_provider

    def _should_transfer(self):
        """
        Decide whether to transfer the file from the source to the destination.
        """
        if self._source_path is None or not self._source_path.is_visible():
            # No source file.  Nothing to transfer.
            return False
        elif self._dest_path is None:
            # Source file exists, but no destination file.  Always transfer.
            return True
        else:
            # Both exist.  Transfer only if the two are different.
            return self.files_are_different(
                self._source_path,
                self._dest_path,
                self._compare_threshold,
                self._compare_version_mode,
                self._newer_file_mode,
            )

    @classmethod
    def files_are_different(
        cls,
        source_path: AbstractSyncPath,
        dest_path: AbstractSyncPath,
        compare_threshold: Optional[int] = None,
        compare_version_mode: CompareVersionMode = CompareVersionMode.MODTIME,
        newer_file_mode: NewerFileSyncMode = NewerFileSyncMode.RAISE_ERROR,
    ):
        """
        Compare two files and determine if the the destination file
        should be replaced by the source file.

        :param b2sdk.v1.AbstractSyncPath source_path: source file object
        :param b2sdk.v1.AbstractSyncPath dest_path: destination file object
        :param int compare_threshold: compare threshold when comparing by time or size
        :param b2sdk.v1.CompareVersionMode compare_version_mode: source file version comparator method
        :param b2sdk.v1.NewerFileSyncMode newer_file_mode: newer destination handling method
        """
        # Optionally set a compare threshold for fuzzy comparison
        compare_threshold = compare_threshold or 0

        # Compare using file name only
        if compare_version_mode == CompareVersionMode.NONE:
            return False

        # Compare using modification time
        elif compare_version_mode == CompareVersionMode.MODTIME:
            # Get the modification time of the latest versions
            source_mod_time = source_path.mod_time
            dest_mod_time = dest_path.mod_time
            diff_mod_time = abs(source_mod_time - dest_mod_time)
            compare_threshold_exceeded = diff_mod_time > compare_threshold

            logger.debug(
                'File %s: source time %s, dest time %s, diff %s, threshold %s, diff > threshold %s',
                source_path.relative_path,
                source_mod_time,
                dest_mod_time,
                diff_mod_time,
                compare_threshold,
                compare_threshold_exceeded,
            )

            if compare_threshold_exceeded:
                # Source is newer
                if dest_mod_time < source_mod_time:
                    return True

                # Source is older
                elif source_mod_time < dest_mod_time:
                    if newer_file_mode == NewerFileSyncMode.REPLACE:
                        return True
                    elif newer_file_mode == NewerFileSyncMode.SKIP:
                        return False
                    else:
                        raise DestFileNewer(
                            dest_path, source_path, cls.DESTINATION_PREFIX, cls.SOURCE_PREFIX
                        )

        # Compare using file size
        elif compare_version_mode == CompareVersionMode.SIZE:
            # Get file size of the latest versions
            source_size = source_path.size
            dest_size = dest_path.size
            diff_size = abs(source_size - dest_size)
            compare_threshold_exceeded = diff_size > compare_threshold

            logger.debug(
                'File %s: source size %s, dest size %s, diff %s, threshold %s, diff > threshold %s',
                source_path.relative_path,
                source_size,
                dest_size,
                diff_size,
                compare_threshold,
                compare_threshold_exceeded,
            )

            # Replace if size difference is over threshold
            return compare_threshold_exceeded
        else:
            raise InvalidArgument('compare_version_mode', 'is invalid option')

    def get_all_actions(self):
        """
        Yield file actions.
        """
        if self._should_transfer():
            yield self._make_transfer_action()
            self._transferred = True

        assert self._dest_path is not None or self._source_path is not None

        for action in self._get_hide_delete_actions():
            yield action

    def _get_hide_delete_actions(self):
        """
        Subclass policy can override this to hide or delete files.
        """
        return []

    def _get_source_mod_time(self):
        return self._source_path.mod_time

    @abstractmethod
    def _make_transfer_action(self):
        """
        Return an action representing transfer of file according to the selected policy.
        """


class DownPolicy(AbstractFileSyncPolicy):
    """
    File is synced down (from the cloud to disk).
    """
    DESTINATION_PREFIX = 'local://'
    SOURCE_PREFIX = 'b2://'

    def _make_transfer_action(self):
        return B2DownloadAction(
            self._source_path,
            self._source_folder.make_full_path(self._source_path.relative_path),
            self._dest_folder.make_full_path(self._source_path.relative_path),
            self._encryption_settings_provider,
        )


class UpPolicy(AbstractFileSyncPolicy):
    """
    File is synced up (from disk the cloud).
    """
    DESTINATION_PREFIX = 'b2://'
    SOURCE_PREFIX = 'local://'

    def _make_transfer_action(self):
        return B2UploadAction(
            self._source_folder.make_full_path(self._source_path.relative_path),
            self._source_path.relative_path,
            self._dest_folder.make_full_path(self._source_path.relative_path),
            self._get_source_mod_time(),
            self._source_path.size,
            self._encryption_settings_provider,
        )


class UpAndDeletePolicy(UpPolicy):
    """
    File is synced up (from disk to the cloud) and the delete flag is SET.
    """

    def _get_hide_delete_actions(self):
        for action in super(UpAndDeletePolicy, self)._get_hide_delete_actions():
            yield action
        for action in make_b2_delete_actions(
            self._source_path,
            self._dest_path,
            self._dest_folder,
            self._transferred,
        ):
            yield action


class UpAndKeepDaysPolicy(UpPolicy):
    """
    File is synced up (from disk to the cloud) and the keepDays flag is SET.
    """

    def _get_hide_delete_actions(self):
        for action in super(UpAndKeepDaysPolicy, self)._get_hide_delete_actions():
            yield action
        for action in make_b2_keep_days_actions(
            self._source_path,
            self._dest_path,
            self._dest_folder,
            self._transferred,
            self._keep_days,
            self._now_millis,
        ):
            yield action


class DownAndDeletePolicy(DownPolicy):
    """
    File is synced down (from the cloud to disk) and the delete flag is SET.
    """

    def _get_hide_delete_actions(self):
        for action in super(DownAndDeletePolicy, self)._get_hide_delete_actions():
            yield action
        if self._dest_path is not None and (
            self._source_path is None or not self._source_path.is_visible()
        ):
            yield LocalDeleteAction(
                self._dest_path.relative_path,
                self._dest_folder.make_full_path(self._dest_path.relative_path)
            )


class DownAndKeepDaysPolicy(DownPolicy):
    """
    File is synced down (from the cloud to disk) and the keepDays flag is SET.
    """
    pass


class CopyPolicy(AbstractFileSyncPolicy):
    """
    File is copied (server-side).
    """
    DESTINATION_PREFIX = 'b2://'
    SOURCE_PREFIX = 'b2://'

    def _make_transfer_action(self):

        return B2CopyAction(
            self._source_folder.make_full_path(self._source_path.relative_path),
            self._source_path,
            self._dest_folder.make_full_path(self._source_path.relative_path),
            self._source_folder.bucket,
            self._dest_folder.bucket,
            self._encryption_settings_provider,
        )


class CopyAndDeletePolicy(CopyPolicy):
    """
    File is copied (server-side) and the delete flag is SET.
    """

    def _get_hide_delete_actions(self):
        for action in super()._get_hide_delete_actions():
            yield action
        for action in make_b2_delete_actions(
            self._source_path,
            self._dest_path,
            self._dest_folder,
            self._transferred,
        ):
            yield action


class CopyAndKeepDaysPolicy(CopyPolicy):
    """
    File is copied (server-side) and the keepDays flag is SET.
    """

    def _get_hide_delete_actions(self):
        for action in super()._get_hide_delete_actions():
            yield action
        for action in make_b2_keep_days_actions(
            self._source_path,
            self._dest_path,
            self._dest_folder,
            self._transferred,
            self._keep_days,
            self._now_millis,
        ):
            yield action


def make_b2_delete_note(version, index, transferred):
    """
    Create a note message for delete action.

    :param b2sdk.v1.FileVersionInfo version: an object which contains file version info
    :param int index: file version index
    :param bool transferred: if True, file has been transferred, False otherwise
    """
    note = ''
    if version.action == 'hide':
        note = '(hide marker)'
    elif transferred or 0 < index:
        note = '(old version)'
    return note


def make_b2_delete_actions(
    source_path: AbstractSyncPath,
    dest_path: AbstractSyncPath,
    dest_folder: AbstractFolder,
    transferred: bool,
):
    """
    Create the actions to delete files stored on B2, which are not present locally.

    :param b2sdk.v1.AbstractSyncPath source_path: source file object
    :param b2sdk.v1.AbstractSyncPath dest_path: destination file object
    :param b2sdk.v1.AbstractFolder dest_folder: destination folder
    :param bool transferred: if True, file has been transferred, False otherwise
    """
    if dest_path is None:
        # B2 does not really store folders, so there is no need to hide
        # them or delete them
        return
    for version_index, version in enumerate(dest_path.all_versions):
        keep = (version_index == 0) and (source_path is not None) and not transferred
        if not keep:
            yield B2DeleteAction(
                dest_path.relative_path,
                dest_folder.make_full_path(dest_path.relative_path),
                version.id_,
                make_b2_delete_note(version, version_index, transferred),
            )


def make_b2_keep_days_actions(
    source_path: AbstractSyncPath,
    dest_path: AbstractSyncPath,
    dest_folder: AbstractFolder,
    transferred: bool,
    keep_days: int,
    now_millis: int,
):
    """
    Create the actions to hide or delete existing versions of a file
    stored in b2.

    When keepDays is set, all files that were visible any time from
    keepDays ago until now must be kept.  If versions were uploaded 5
    days ago, 15 days ago, and 25 days ago, and the keepDays is 10,
    only the 25-day old version can be deleted.  The 15 day-old version
    was visible 10 days ago.

    :param b2sdk.v1.AbstractSyncPath source_path: source file object
    :param b2sdk.v1.AbstractSyncPath dest_path: destination file object
    :param b2sdk.v1.AbstractFolder dest_folder: destination folder object
    :param bool transferred: if True, file has been transferred, False otherwise
    :param int keep_days: how many days to keep a file
    :param int now_millis: current time in milliseconds
    """
    deleting = False
    if dest_path is None:
        # B2 does not really store folders, so there is no need to hide
        # them or delete them
        return
    for version_index, version in enumerate(dest_path.all_versions):
        # How old is this version?
        age_days = (now_millis - version.mod_time_millis) / ONE_DAY_IN_MS

        # Mostly, the versions are ordered by time, newest first,
        # BUT NOT ALWAYS. The mod time we have is the src_last_modified_millis
        # from the file info (if present), or the upload start time
        # (if not present). The user-specified src_last_modified_millis
        # may not be in order. Because of that, we no longer
        # assert that age_days is non-decreasing.
        #
        # Note that if there is an out-of-order date that is old enough
        # to trigger deletions, all of the versions uploaded before that
        # (the ones after it in the list) will be deleted, even if they
        # aren't over the age threshold.

        # Do we need to hide this version?
        if version_index == 0 and source_path is None and version.action == 'upload':
            yield B2HideAction(
                dest_path.relative_path, dest_folder.make_full_path(dest_path.relative_path)
            )

        # Can we start deleting? Once we start deleting, all older
        # versions will also be deleted.
        if version.action == 'hide':
            if keep_days < age_days:
                deleting = True

        # Delete this version
        if deleting:
            yield B2DeleteAction(
                dest_path.relative_path,
                dest_folder.make_full_path(dest_path.relative_path),
                version.id_,
                make_b2_delete_note(version, version_index, transferred),
            )

        # Can we start deleting with the next version, based on the
        # age of this one?
        if keep_days < age_days:
            deleting = True
