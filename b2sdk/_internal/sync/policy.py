######################################################################
#
# File: b2sdk/_internal/sync/policy.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from typing import cast

from ..exception import DestFileNewer
from ..scan.exception import InvalidArgument
from ..scan.folder import AbstractFolder, B2Folder
from ..scan.path import AbstractPath, B2Path
from ..transfer.outbound.upload_source import UploadMode
from .action import (
    B2CopyAction,
    B2DeleteAction,
    B2DownloadAction,
    B2HideAction,
    B2IncrementalUploadAction,
    B2UploadAction,
    LocalDeleteAction,
)
from .encryption_provider import (
    SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    AbstractSyncEncryptionSettingsProvider,
)

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
        source_path: AbstractPath | None,
        source_folder: AbstractFolder,
        dest_path: AbstractPath | None,
        dest_folder: AbstractFolder,
        now_millis: int,
        keep_days: int,
        newer_file_mode: NewerFileSyncMode,
        compare_threshold: int,
        compare_version_mode: CompareVersionMode = CompareVersionMode.MODTIME,
        encryption_settings_provider:
        AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
        upload_mode: UploadMode = UploadMode.FULL,
        absolute_minimum_part_size: int | None = None,
    ):
        """
        :param source_path: source file object
        :param source_folder: source folder object
        :param dest_path: destination file object
        :param dest_folder: destination folder object
        :param now_millis: current time in milliseconds
        :param keep_days: days to keep before delete
        :param newer_file_mode: setting which determines handling for destination files newer than on the source
        :param compare_threshold: when comparing with size or time for sync
        :param compare_version_mode: how to compare source and destination files
        :param encryption_settings_provider: encryption setting provider
        :param upload_mode: file upload mode
        :param absolute_minimum_part_size: minimum file part size that can be uploaded to the server
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
        self._upload_mode = upload_mode
        self._absolute_minimum_part_size = absolute_minimum_part_size

    def _should_transfer(self) -> bool:
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
        source_path: AbstractPath,
        dest_path: AbstractPath,
        compare_threshold: int | None = None,
        compare_version_mode: CompareVersionMode = CompareVersionMode.MODTIME,
        newer_file_mode: NewerFileSyncMode = NewerFileSyncMode.RAISE_ERROR,
    ):
        """
        Compare two files and determine if the the destination file
        should be replaced by the source file.

        :param b2sdk.v2.AbstractPath source_path: source file object
        :param b2sdk.v2.AbstractPath dest_path: destination file object
        :param int compare_threshold: compare threshold when comparing by time or size
        :param b2sdk.v2.CompareVersionMode compare_version_mode: source file version comparator method
        :param b2sdk.v2.NewerFileSyncMode newer_file_mode: newer destination handling method
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

        yield from self._get_hide_delete_actions()

    def _get_hide_delete_actions(self):
        """
        Subclass policy can override this to hide or delete files.
        """
        return []

    def _get_source_mod_time(self) -> int:
        if self._source_path is None:
            return 0
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
            cast(B2Path, self._source_path),
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
        # Find out if we want to append with new bytes or replace completely
        if self._upload_mode == UploadMode.INCREMENTAL and self._dest_path:
            return B2IncrementalUploadAction(
                self._source_folder.make_full_path(self._source_path.relative_path),
                self._source_path.relative_path,
                self._dest_folder.make_full_path(self._source_path.relative_path),
                self._get_source_mod_time(),
                self._source_path.size,
                self._encryption_settings_provider,
                cast(B2Path, self._dest_path).selected_version,
                self._absolute_minimum_part_size,
            )
        else:
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
        yield from super()._get_hide_delete_actions()
        yield from make_b2_delete_actions(
            self._source_path,
            self._dest_path,
            self._dest_folder,
            self._transferred,
        )


class UpAndKeepDaysPolicy(UpPolicy):
    """
    File is synced up (from disk to the cloud) and the keepDays flag is SET.
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


class DownAndDeletePolicy(DownPolicy):
    """
    File is synced down (from the cloud to disk) and the delete flag is SET.
    """

    def _get_hide_delete_actions(self):
        yield from super()._get_hide_delete_actions()
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
            cast(B2Path, self._source_path),
            self._dest_folder.make_full_path(self._source_path.relative_path),
            cast(B2Folder, self._source_folder).bucket,
            cast(B2Folder, self._dest_folder).bucket,
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

    :param b2sdk.v2.FileVersion version: an object which contains file version info
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
    source_path: AbstractPath | None,
    dest_path: B2Path | None,
    dest_folder: AbstractFolder,
    transferred: bool,
):
    """
    Create the actions to delete files stored on B2, which are not present locally.

    :param source_path: source file object
    :param dest_path: destination file object
    :param dest_folder: destination folder
    :param transferred: if True, file has been transferred, False otherwise
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
    source_path: AbstractPath | None,
    dest_path: B2Path | None,
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
    only the 25 day-old version can be deleted.  The 15 day-old version
    was visible 10 days ago.

    :param source_path: source file object
    :param dest_path: destination file object
    :param dest_folder: destination folder object
    :param transferred: if True, file has been transferred, False otherwise
    :param keep_days: how many days to keep a file
    :param now_millis: current time in milliseconds
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
        # to trigger deletions, all the versions uploaded before that
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
