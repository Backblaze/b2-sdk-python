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

import six
import logging

from ..exception import CommandError, DestFileNewer
from .action import LocalDeleteAction, B2DeleteAction, B2DownloadAction, B2HideAction, B2UploadAction

ONE_DAY_IN_MS = 24 * 60 * 60 * 1000

logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class AbstractFileSyncPolicy(object):
    """
    Abstract policy class
    """
    DESTINATION_PREFIX = NotImplemented
    SOURCE_PREFIX = NotImplemented

    def __init__(self, source_file, source_folder, dest_file, dest_folder, now_millis, args):
        """
        :param source_file: source file object
        :type source_file: b2sdk.sync.file.File
        :param source_folder: source folder object
        :type source_folder: b2sdk.sync.folder.AbstractFolder
        :param dest_file: destination file object
        :type dest_file: b2sdk.sync.file.File
        :param dest_folder: destination folder object
        :type dest_folder: b2sdk.sync.folder.AbstractFolder
        :param now_millis: current time in milliseconds
        :type now_millis: int
        :param args: an object which holds command line arguments
        """
        self._source_file = source_file
        self._source_folder = source_folder
        self._dest_file = dest_file
        self._delete = args.delete
        self._keepDays = args.keepDays
        self._args = args
        self._dest_folder = dest_folder
        self._now_millis = now_millis
        self._transferred = False

    def _should_transfer(self):
        """
        Decides whether to transfer the file from the source to the destination.
        """
        if self._source_file is None:
            # No source file.  Nothing to transfer.
            return False
        elif self._dest_file is None:
            # Source file exists, but no destination file.  Always transfer.
            return True
        else:
            # Both exist.  Transfer only if the two are different.
            return self.files_are_different(self._source_file, self._dest_file, self._args)

    @classmethod
    def files_are_different(cls, source_file, dest_file, args):
        """
        Compare two files and determine if the the destination file
        should be replaced by the source file.

        :param source_file: source file object
        :type source_file: b2sdk.sync.file.File
        :param dest_file: destination file object
        :type dest_file: b2sdk.sync.file.File
        :param args: an object which holds command line arguments
        :rtype: bool
        """

        # Compare using modification time by default
        compareVersions = args.compareVersions or 'modTime'

        # Optionally set a compare threshold for fuzzy comparison
        compareThreshold = args.compareThreshold or 0
        if compareThreshold < 0:
            raise CommandError('Invalid option for --compareThreshold (must be a positive integer')

        # Compare using file name only
        if compareVersions == 'none':
            return False

        # Compare using modification time
        elif compareVersions == 'modTime':
            # Get the modification time of the latest versions
            source_mod_time = source_file.latest_version().mod_time
            dest_mod_time = dest_file.latest_version().mod_time
            diff_mod_time = abs(source_mod_time - dest_mod_time)
            compare_threshold_exceeded = diff_mod_time > compareThreshold

            logger.debug(
                'File %s: source time %s, dest time %s, diff %s, threshold %s, diff > threshold %s',
                source_file.name, source_mod_time, dest_mod_time, diff_mod_time, compareThreshold,
                compare_threshold_exceeded
            )

            if compare_threshold_exceeded:
                # Source is newer
                if dest_mod_time < source_mod_time:
                    return True

                # Source is older
                elif source_mod_time < dest_mod_time:
                    if args.replaceNewer:
                        return True
                    elif args.skipNewer:
                        return False
                    else:
                        raise DestFileNewer(
                            dest_file, source_file, cls.DESTINATION_PREFIX, cls.SOURCE_PREFIX
                        )

        # Compare using file size
        elif compareVersions == 'size':
            # Get file size of the latest versions
            source_size = source_file.latest_version().size
            dest_size = dest_file.latest_version().size
            diff_size = abs(source_size - dest_size)
            compare_threshold_exceeded = diff_size > compareThreshold

            logger.debug(
                'File %s: source size %s, dest size %s, diff %s, threshold %s, diff > threshold %s',
                source_file.name, source_size, dest_size, diff_size, compareThreshold,
                compare_threshold_exceeded
            )

            # Replace if size difference is over threshold
            return compare_threshold_exceeded
        else:
            raise CommandError('Invalid option for --compareVersions')

    def get_all_actions(self):
        """
        Yield file actions.
        """
        if self._should_transfer():
            yield self._make_transfer_action()
            self._transferred = True

        assert self._dest_file is not None or self._source_file is not None

        for action in self._get_hide_delete_actions():
            yield action

    def _get_hide_delete_actions(self):
        """
        subclass policy can override this to hide or delete files
        """
        return []

    def _get_source_mod_time(self):
        return self._source_file.latest_version().mod_time

    @abstractmethod
    def _make_transfer_action(self):
        """ return an action representing transfer of file according to the selected policy """


class DownPolicy(AbstractFileSyncPolicy):
    """
    File is synced down (from the cloud to disk)
    """
    DESTINATION_PREFIX = 'local://'
    SOURCE_PREFIX = 'b2://'

    def _make_transfer_action(self):
        return B2DownloadAction(
            self._source_file.name,
            self._source_folder.make_full_path(self._source_file.name),
            self._source_file.latest_version().id_,
            self._dest_folder.make_full_path(self._source_file.name),
            self._get_source_mod_time(),
            self._source_file.latest_version().size,
        )


class UpPolicy(AbstractFileSyncPolicy):
    """
    File is synced up (from disk the cloud)
    """
    DESTINATION_PREFIX = 'b2://'
    SOURCE_PREFIX = 'local://'

    def _make_transfer_action(self):
        return B2UploadAction(
            self._source_folder.make_full_path(self._source_file.name),
            self._source_file.name,
            self._dest_folder.make_full_path(self._source_file.name),
            self._get_source_mod_time(),
            self._source_file.latest_version().size,
        )


class UpAndDeletePolicy(UpPolicy):
    """
    File is synced up (from disk to the cloud) and the delete flag is SET
    """

    def _get_hide_delete_actions(self):
        for action in super(UpAndDeletePolicy, self)._get_hide_delete_actions():
            yield action
        for action in make_b2_delete_actions(
            self._source_file,
            self._dest_file,
            self._dest_folder,
            self._transferred,
        ):
            yield action


class UpAndKeepDaysPolicy(UpPolicy):
    """
    File is synced up (from disk to the cloud) and the keepDays flag is SET
    """

    def _get_hide_delete_actions(self):
        for action in super(UpAndKeepDaysPolicy, self)._get_hide_delete_actions():
            yield action
        for action in make_b2_keep_days_actions(
            self._source_file, self._dest_file, self._dest_folder, self._transferred,
            self._keepDays, self._now_millis
        ):
            yield action


class DownAndDeletePolicy(DownPolicy):
    """
    File is synced down (from the cloud to disk) and the delete flag is SET
    """

    def _get_hide_delete_actions(self):
        for action in super(DownAndDeletePolicy, self)._get_hide_delete_actions():
            yield action
        if self._dest_file is not None and self._source_file is None:
            # Local files have either 0 or 1 versions.  If the file is there,
            # it must have exactly 1 version.
            yield LocalDeleteAction(self._dest_file.name, self._dest_file.versions[0].id_)


class DownAndKeepDaysPolicy(DownPolicy):
    """
    File is synced down (from the cloud to disk) and the keepDays flag is SET
    """
    pass


def make_b2_delete_note(version, index, transferred):
    """
    Create a note message for delete action

    :param version: an object which contains file version info
    :param index: file version index
    :type index: int
    :param transferred: if True, file has been transferred,
                        False otherwise
    :type transferred: bool
    """
    note = ''
    if version.action == 'hide':
        note = '(hide marker)'
    elif transferred or 0 < index:
        note = '(old version)'
    return note


def make_b2_delete_actions(source_file, dest_file, dest_folder, transferred):
    """
    Creates the actions to delete files stored on B2, which are not present locally.

    :param source_file: source file object
    :type source_file: b2sdk.sync.file.File
    :param dest_file: destination file object
    :type dest_file: b2sdk.sync.file.File
    :param dest_folder: destination folder
    :type dest_folder: b2sdk.sync.folder.AbstractFolder
    :param transferred: if True, file has been transferred,
                        False otherwise
    :type transferred: bool
    """
    if dest_file is None:
        # B2 does not really store folders, so there is no need to hide
        # them or delete them
        return
    for version_index, version in enumerate(dest_file.versions):
        keep = (version_index == 0) and (source_file is not None) and not transferred
        if not keep:
            yield B2DeleteAction(
                dest_file.name,
                dest_folder.make_full_path(dest_file.name),
                version.id_,
                make_b2_delete_note(version, version_index, transferred),
            )


def make_b2_keep_days_actions(
    source_file, dest_file, dest_folder, transferred, keep_days, now_millis
):
    """
    Creates the actions to hide or delete existing versions of a file
    stored in b2.

    When keepDays is set, all files that were visible any time from
    keepDays ago until now must be kept.  If versions were uploaded 5
    days ago, 15 days ago, and 25 days ago, and the keepDays is 10,
    only the 25-day old version can be deleted.  The 15 day-old version
    was visible 10 days ago.

    :param source_file: source file object
    :type source_file: b2sdk.sync.file.File
    :param dest_file: destination file object
    :type dest_file: b2sdk.sync.file.File
    :param dest_folder: destination folder object
    :type dest_folder: b2sdk.sync.folder.AbstractFolder
    :param transferred: if True, file has been transferred,
                        False otherwise
    :type transferred: bool
    :param keep_days: how many days to keep a file
    :type keep_days: int
    :param now_millis: current time in milliseconds
    :type now_millis: int
    """
    deleting = False
    if dest_file is None:
        # B2 does not really store folders, so there is no need to hide
        # them or delete them
        return
    for version_index, version in enumerate(dest_file.versions):
        # How old is this version?
        age_days = (now_millis - version.mod_time) / ONE_DAY_IN_MS

        # Mostly, the versions are ordered by time, newest first,
        # BUT NOT ALWAYS.  The mod time we have is the src_last_modified_millis
        # from the file info (if present), or the upload start time
        # (if not present).  The user-specified src_last_modified_millis
        # may not be in order.  Because of that, we no longer
        # assert that age_days is non-decreasing.
        #
        # Note that if there is an out-of-order date that is old enough
        # to trigger deletions, all of the versions uploaded before that
        # (the ones after it in the list) will be deleted, even if they
        # aren't over the age threshold.

        # Do we need to hide this version?
        if version_index == 0 and source_file is None and version.action == 'upload':
            yield B2HideAction(dest_file.name, dest_folder.make_full_path(dest_file.name))

        # Can we start deleting?  Once we start deleting, all older
        # versions will also be deleted.
        if version.action == 'hide':
            if keep_days < age_days:
                deleting = True

        # Delete this version
        if deleting:
            yield B2DeleteAction(
                dest_file.name,
                dest_folder.make_full_path(dest_file.name),
                version.id_,
                make_b2_delete_note(version, version_index, transferred),
            )

        # Can we start deleting with the next version, based on the
        # age of this one?
        if keep_days < age_days:
            deleting = True
