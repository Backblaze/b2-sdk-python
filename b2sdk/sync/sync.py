######################################################################
#
# File: b2sdk/sync/sync.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import concurrent.futures as futures
from enum import Enum, unique

from ..bounded_queue_executor import BoundedQueueExecutor
from .encryption_provider import AbstractSyncEncryptionSettingsProvider, SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER
from .exception import InvalidArgument, IncompleteSync
from .folder import AbstractFolder
from .path import AbstractSyncPath
from .policy import CompareVersionMode, NewerFileSyncMode
from .policy_manager import POLICY_MANAGER, SyncPolicyManager
from .report import SyncReport
from .scan_policies import DEFAULT_SCAN_MANAGER

logger = logging.getLogger(__name__)


def next_or_none(iterator):
    """
    Return the next item from the iterator, or None if there are no more.
    """
    try:
        return next(iterator)
    except StopIteration:
        return None


def zip_folders(folder_a, folder_b, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
    """
    Iterate over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.

    :param b2sdk.sync.folder.AbstractFolder folder_a: first folder object.
    :param b2sdk.sync.folder.AbstractFolder folder_b: second folder object.
    :param reporter: reporter object
    :param policies_manager: policies manager object
    :return: yields two element tuples
    """

    iter_a = folder_a.all_files(reporter, policies_manager)
    iter_b = folder_b.all_files(reporter)

    current_a = next_or_none(iter_a)
    current_b = next_or_none(iter_b)

    while current_a is not None or current_b is not None:
        if current_a is None:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        elif current_b is None:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_a.relative_path < current_b.relative_path:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_b.relative_path < current_a.relative_path:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        else:
            assert current_a.relative_path == current_b.relative_path
            yield (current_a, current_b)
            current_a = next_or_none(iter_a)
            current_b = next_or_none(iter_b)


def count_files(local_folder, reporter, policies_manager):
    """
    Count all of the files in a local folder.

    :param b2sdk.sync.folder.AbstractFolder local_folder: a folder object.
    :param reporter: reporter object
    """
    # Don't pass in a reporter to all_files.  Broken symlinks will be reported
    # during the next pass when the source and dest files are compared.
    for _ in local_folder.all_files(None, policies_manager=policies_manager):
        reporter.update_total(1)
    reporter.end_total()


@unique
class KeepOrDeleteMode(Enum):
    """ Mode of dealing with old versions of files on the destination """
    DELETE = 301  #: delete the old version as soon as the new one has been uploaded
    KEEP_BEFORE_DELETE = 302  #: keep the old versions of the file for a configurable number of days before deleting them, always keeping the newest version
    NO_DELETE = 303  #: keep old versions of the file, do not delete anything


class Synchronizer:
    """
    Copies multiple "files" from source to destination.  Optionally
    deletes or hides destination files that the source does not have.

    The synchronizer can copy files:

    - From a B2 bucket to a local destination.
    - From a local source to a B2 bucket.
    - From one B2 bucket to another.
    - Between different folders in the same B2 bucket.
      It will sync only the latest versions of files.

    By default, the synchronizer:

    - Fails when the specified source directory doesn't exist or is empty.
      (see ``allow_empty_source`` argument)
    - Fails when the source is newer.
      (see ``newer_file_mode`` argument)
    - Doesn't delete a file if it's present on the destination but not on the source.
      (see ``keep_days_or_delete`` and ``keep_days`` arguments)
    - Compares files based on modification time.
      (see ``compare_version_mode`` and ``compare_threshold`` arguments)
    """

    def __init__(
        self,
        max_workers,
        policies_manager=DEFAULT_SCAN_MANAGER,
        dry_run=False,
        allow_empty_source=False,
        newer_file_mode=NewerFileSyncMode.RAISE_ERROR,
        keep_days_or_delete=KeepOrDeleteMode.NO_DELETE,
        compare_version_mode=CompareVersionMode.MODTIME,
        compare_threshold=None,
        keep_days=None,
    ):
        """
        Initialize synchronizer class and validate arguments

        :param int max_workers: max number of workers
        :param policies_manager: policies manager object
        :param bool dry_run: test mode, does not actually transfer/delete when enabled
        :param bool allow_empty_source: if True, do not check whether source folder is empty
        :param b2sdk.v1.NewerFileSyncMode newer_file_mode: setting which determines handling for destination files newer than on the source
        :param b2sdk.v1.KeepOrDeleteMode keep_days_or_delete: setting which determines if we should delete or not delete or keep for `keep_days`
        :param b2sdk.v1.CompareVersionMode compare_version_mode: how to compare the source and destination files to find new ones
        :param int compare_threshold: should be greater than 0, default is 0
        :param int keep_days: if keep_days_or_delete is `b2sdk.v1.KeepOrDeleteMode.KEEP_BEFORE_DELETE`, then this should be greater than 0
        """
        self.newer_file_mode = newer_file_mode
        self.keep_days_or_delete = keep_days_or_delete
        self.keep_days = keep_days
        self.compare_version_mode = compare_version_mode
        self.compare_threshold = compare_threshold or 0
        self.dry_run = dry_run
        self.allow_empty_source = allow_empty_source
        self.policies_manager = policies_manager
        self.max_workers = max_workers
        self._validate()

    def _validate(self):
        if self.compare_threshold < 0:
            raise InvalidArgument('compare_threshold', 'must be a positive integer')

        if self.newer_file_mode not in tuple(NewerFileSyncMode):
            raise InvalidArgument(
                'newer_file_mode',
                'must be one of :%s' % NewerFileSyncMode.__members__,
            )

        if self.keep_days_or_delete not in tuple(KeepOrDeleteMode):
            raise InvalidArgument(
                'keep_days_or_delete',
                'must be one of :%s' % KeepOrDeleteMode.__members__,
            )

        if self.keep_days_or_delete == KeepOrDeleteMode.KEEP_BEFORE_DELETE and self.keep_days is None:
            raise InvalidArgument(
                'keep_days',
                'is required when keep_days_or_delete is %s' % KeepOrDeleteMode.KEEP_BEFORE_DELETE,
            )

        if self.compare_version_mode not in tuple(CompareVersionMode):
            raise InvalidArgument(
                'compare_version_mode',
                'must be one of :%s' % CompareVersionMode.__members__,
            )

    def sync_folders(
        self,
        source_folder,
        dest_folder,
        now_millis,
        reporter,
        encryption_settings_provider:
        AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    ):
        """
        Syncs two folders.  Always ensures that every file in the
        source is also in the destination.  Deletes any file versions
        in the destination older than history_days.

        :param b2sdk.sync.folder.AbstractFolder source_folder: source folder object
        :param b2sdk.sync.folder.AbstractFolder dest_folder: destination folder object
        :param int now_millis: current time in milliseconds
        :param b2sdk.sync.report.SyncReport,None reporter: progress reporter
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        source_type = source_folder.folder_type()
        dest_type = dest_folder.folder_type()

        if source_type != 'b2' and dest_type != 'b2':
            raise ValueError('Sync between two local folders is not supported!')

        # For downloads, make sure that the target directory is there.
        if dest_type == 'local' and not self.dry_run:
            dest_folder.ensure_present()

        if source_type == 'local' and not self.allow_empty_source:
            source_folder.ensure_non_empty()

        # Make an executor to count files and run all of the actions. This is
        # not the same as the executor in the API object which is used for
        # uploads. The tasks in this executor wait for uploads. Putting them
        # in the same thread pool could lead to deadlock.
        #
        # We use an executor with a bounded queue to avoid using up lots of memory
        # when syncing lots of files.
        unbounded_executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        queue_limit = self.max_workers + 1000
        sync_executor = BoundedQueueExecutor(unbounded_executor, queue_limit=queue_limit)

        if source_type == 'local' and reporter is not None:
            # Start the thread that counts the local files. That's the operation
            # that should be fastest, and it provides scale for the progress reporting.
            sync_executor.submit(count_files, source_folder, reporter, self.policies_manager)

        # Bucket for scheduling actions.
        # For bucket-to-bucket sync, the bucket for the API calls should be the destination.
        action_bucket = None
        if dest_type == 'b2':
            action_bucket = dest_folder.bucket
        elif source_type == 'b2':
            action_bucket = source_folder.bucket

        # Schedule each of the actions.
        for action in self._make_folder_sync_actions(
            source_folder,
            dest_folder,
            now_millis,
            reporter,
            self.policies_manager,
            encryption_settings_provider,
        ):
            logging.debug('scheduling action %s on bucket %s', action, action_bucket)
            sync_executor.submit(action.run, action_bucket, reporter, self.dry_run)

        # Wait for everything to finish
        sync_executor.shutdown()
        if sync_executor.get_num_exceptions() != 0:
            raise IncompleteSync('sync is incomplete')

    def _make_folder_sync_actions(
        self,
        source_folder: AbstractFolder,
        dest_folder: AbstractFolder,
        now_millis: int,
        reporter: SyncReport,
        policies_manager: SyncPolicyManager = DEFAULT_SCAN_MANAGER,
        encryption_settings_provider:
        AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    ):
        """
        Yield a sequence of actions that will sync the destination
        folder to the source folder.

        :param b2sdk.v1.AbstractFolder source_folder: source folder object
        :param b2sdk.v1.AbstractFolder dest_folder: destination folder object
        :param int now_millis: current time in milliseconds
        :param b2sdk.v1.SyncReport reporter: reporter object
        :param b2sdk.v1.SyncPolicyManager policies_manager: policies manager object
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        if self.keep_days_or_delete == KeepOrDeleteMode.KEEP_BEFORE_DELETE and dest_folder.folder_type(
        ) == 'local':
            raise InvalidArgument('keep_days_or_delete', 'cannot be used for local files')

        source_type = source_folder.folder_type()
        dest_type = dest_folder.folder_type()
        sync_type = '%s-to-%s' % (source_type, dest_type)
        if source_type != 'b2' and dest_type != 'b2':
            raise ValueError('Sync between two local folders is not supported!')

        total_files = 0
        total_bytes = 0
        for source_path, dest_path in zip_folders(
            source_folder,
            dest_folder,
            reporter,
            policies_manager,
        ):
            if source_path is None:
                logger.debug('determined that %s is not present on source', dest_path)
            elif dest_path is None:
                logger.debug('determined that %s is not present on destination', source_path)

            if source_path is not None:
                if source_type == 'b2':
                    # For buckets we don't want to count files separately as it would require
                    # more API calls. Instead, we count them when comparing.
                    reporter.update_total(1)
                reporter.update_compare(1)

            for action in self._make_file_sync_actions(
                sync_type,
                source_path,
                dest_path,
                source_folder,
                dest_folder,
                now_millis,
                encryption_settings_provider,
            ):
                total_files += 1
                total_bytes += action.get_bytes()
                yield action

        if reporter is not None:
            if source_type == 'b2':
                # For buckets we don't want to count files separately as it would require
                # more API calls. Instead, we count them when comparing.
                reporter.end_total()
            reporter.end_compare(total_files, total_bytes)

    def _make_file_sync_actions(
        self,
        sync_type: str,
        source_path: AbstractSyncPath,
        dest_path: AbstractSyncPath,
        source_folder: AbstractFolder,
        dest_folder: AbstractFolder,
        now_millis: int,
        encryption_settings_provider:
        AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    ):
        """
        Yields the sequence of actions needed to sync the two files

        :param str sync_type: synchronization type
        :param b2sdk.v1.AbstractSyncPath source_path: source file object
        :param b2sdk.v1.AbstractSyncPath dest_path: destination file object
        :param b2sdk.v1.AbstractFolder source_folder: a source folder object
        :param b2sdk.v1.AbstractFolder dest_folder: a destination folder object
        :param int now_millis: current time in milliseconds
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        delete = self.keep_days_or_delete == KeepOrDeleteMode.DELETE
        keep_days = self.keep_days

        policy = POLICY_MANAGER.get_policy(
            sync_type,
            source_path,
            source_folder,
            dest_path,
            dest_folder,
            now_millis,
            delete,
            keep_days,
            self.newer_file_mode,
            self.compare_threshold,
            self.compare_version_mode,
            encryption_settings_provider=encryption_settings_provider,
        )
        return policy.get_all_actions()
