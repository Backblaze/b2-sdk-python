######################################################################
#
# File: b2sdk/sync/sync.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import division

import logging
import six

from ..bounded_queue_executor import BoundedQueueExecutor
from .exception import InvalidArgument, IncompleteSync
from .policy import AbstractFileSyncPolicy
from .policy_manager import POLICY_MANAGER
from .scan_policies import DEFAULT_SCAN_MANAGER

try:
    import concurrent.futures as futures
except ImportError:
    import futures

logger = logging.getLogger(__name__)


def next_or_none(iterator):
    """
    Return the next item from the iterator, or None if there are no more.
    """
    try:
        return six.advance_iterator(iterator)
    except StopIteration:
        return None


def zip_folders(folder_a, folder_b, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
    """
    Iterate over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.

    :param folder_a: first folder object.
    :type folder_a: b2sdk.sync.folder.AbstractFolder
    :param folder_b: second folder object.
    :type folder_b: b2sdk.sync.folder.AbstractFolder
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
        elif current_a.name < current_b.name:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_b.name < current_a.name:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        else:
            assert current_a.name == current_b.name
            yield (current_a, current_b)
            current_a = next_or_none(iter_a)
            current_b = next_or_none(iter_b)


def count_files(local_folder, reporter):
    """
    Count all of the files in a local folder.

    :param local_folder: a folder object.
    :type local_folder: b2sdk.sync.folder.AbstractFolder
    :param reporter: reporter object
    """
    # Don't pass in a reporter to all_files.  Broken symlinks will be reported
    # during the next pass when the source and dest files are compared.
    for _ in local_folder.all_files(None):
        reporter.update_local(1)
    reporter.end_local()


class Synchronizer(object):
    DELETE_MODE = 301
    KEEP_DAYS_MODE = 302

    KEEP_OR_DELETE_MODES = [DELETE_MODE, KEEP_DAYS_MODE]

    def __init__(
        self,
        no_progress,
        max_workers,
        policies_manager=DEFAULT_SCAN_MANAGER,
        dry_run=False,
        allow_empty_source=False,
        newer_file_mode=None,
        keep_days_or_delete=None,
        compare_version_mode=None,
        compare_threshold=None,
        keep_days=None,
    ):
        """
        Initialize synchronizer class and validate arguments

        :param no_progress: if True, do not show progress
        :type no_progress: bool
        :param max_workers: max number of workers
        :type max_workers: int
        :param policies_manager: policies manager object
        :param dry_run:
        :type dry_run: bool
        :param allow_empty_source: if True, do not check whether source folder is empty
        :type allow_empty_source: bool
        :param newer_file_mode: one of 101 (Skip) or 102 (Replace), can be None
        :type newer_file_mode: int
        :param keep_days_or_delete: one of 301 (Delete policy), 302 (keep for days policy), can be None
        :type keep_days_or_delete: int
        :param compare_version_mode: one of 201 (Modification time), 202 (Size), 203 (none), default is 201
        :type compare_version_mode: int
        :param compare_threshold: should be greater than 0, default is 0
        :type compare_threshold: int
        :param keep_days: if keep_days_or_delete is 302, then this should be greater than 0
        :type keep_days: int
        """
        self.newer_file_mode = newer_file_mode
        self.keep_days_or_delete = keep_days_or_delete
        self.keep_days = keep_days
        self.compare_version_mode = compare_version_mode or AbstractFileSyncPolicy.COMPARE_VERSION_MODTIME
        self.compare_threshold = compare_threshold or 0
        self.dry_run = dry_run
        self.allow_empty_source = allow_empty_source
        self.policies_manager = policies_manager
        self.max_workers = max_workers
        self.no_progress = no_progress
        self._validate()

    def _validate(self):
        if self.compare_threshold < 0:
            raise InvalidArgument('compare_threshold', 'must be a positive integer')

        if self.newer_file_mode and self.newer_file_mode not in AbstractFileSyncPolicy.NEWER_FILE_MODES:
            raise InvalidArgument(
                'newer_file_mode',
                'must be one of :%s' % AbstractFileSyncPolicy.NEWER_FILE_MODES,
            )

        if self.keep_days_or_delete and self.keep_days_or_delete not in self.KEEP_OR_DELETE_MODES:
            raise InvalidArgument(
                'keep_days_or_delete',
                'must be one of :%s' % self.KEEP_OR_DELETE_MODES,
            )

        if self.keep_days_or_delete == self.KEEP_DAYS_MODE and self.keep_days is None:
            raise InvalidArgument(
                'keep_days',
                'is required when keep_days_or_delete is %s' % self.KEEP_DAYS_MODE,
            )

        if self.compare_version_mode and self.compare_version_mode not in AbstractFileSyncPolicy.COMPARE_VERSION_MODES:
            raise InvalidArgument(
                'compare_version_mode',
                'must be one of :%s' % AbstractFileSyncPolicy.COMPARE_VERSION_MODES,
            )

    def sync_folders(self, source_folder, dest_folder, now_millis, reporter):
        """
        Syncs two folders.  Always ensures that every file in the
        source is also in the destination.  Deletes any file versions
        in the destination older than history_days.

        :param source_folder: source folder object
        :type source_folder: b2sdk.sync.folder.AbstractFolder
        :param dest_folder: destination folder object
        :type dest_folder: b2sdk.sync.folder.AbstractFolder
        :param now_millis: current time in milliseconds
        :type now_millis: int
        """
        # For downloads, make sure that the target directory is there.
        if dest_folder.folder_type() == 'local' and not self.dry_run:
            dest_folder.ensure_present()

        if source_folder.folder_type() == 'local' and not self.allow_empty_source:
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

        # First, start the thread that counts the local files. That's the operation
        # that should be fastest, and it provides scale for the progress reporting.
        local_folder = None
        if source_folder.folder_type() == 'local':
            local_folder = source_folder
        if dest_folder.folder_type() == 'local':
            local_folder = dest_folder
        if local_folder is None:
            raise ValueError('neither folder is a local folder')
        if reporter:
            sync_executor.submit(count_files, local_folder, reporter)

        # Schedule each of the actions
        bucket = None
        if source_folder.folder_type() == 'b2':
            bucket = source_folder.bucket
        if dest_folder.folder_type() == 'b2':
            bucket = dest_folder.bucket
        if bucket is None:
            raise ValueError('neither folder is a b2 folder')
        total_files = 0
        total_bytes = 0
        for action in self.make_folder_sync_actions(
            source_folder, dest_folder, now_millis, reporter, self.policies_manager
        ):
            logging.debug('scheduling action %s on bucket %s', action, bucket)
            sync_executor.submit(action.run, bucket, reporter, self.dry_run)
            total_files += 1
            total_bytes += action.get_bytes()
        if reporter:
            reporter.end_compare(total_files, total_bytes)
        # Wait for everything to finish
        sync_executor.shutdown()
        if sync_executor.get_num_exceptions() != 0:
            raise IncompleteSync('sync is incomplete')

    def make_folder_sync_actions(
        self,
        source_folder,
        dest_folder,
        now_millis,
        reporter,
        policies_manager=DEFAULT_SCAN_MANAGER,
    ):
        """
        Yield a sequence of actions that will sync the destination
        folder to the source folder.

        :param source_folder: source folder object
        :type source_folder: b2sdk.sync.folder.AbstractFolder
        :param dest_folder: destination folder object
        :type dest_folder: b2sdk.sync.folder.AbstractFolder
        :param now_millis: current time in milliseconds
        :type now_millis: int
        :param reporter: reporter object
        :param policies_manager: policies manager object
        """
        if self.keep_days_or_delete == self.KEEP_DAYS_MODE and dest_folder.folder_type() == 'local':
            raise InvalidArgument('keep_days_or_delete', 'cannot be used for local files')

        source_type = source_folder.folder_type()
        dest_type = dest_folder.folder_type()
        sync_type = '%s-to-%s' % (source_type, dest_type)
        if (source_type, dest_type) not in [('b2', 'local'), ('local', 'b2')]:
            raise NotImplementedError("Sync support only local-to-b2 and b2-to-local")

        for source_file, dest_file in zip_folders(
            source_folder,
            dest_folder,
            reporter,
            policies_manager,
        ):
            if source_file is None:
                logger.debug('determined that %s is not present on source', dest_file)
            elif dest_file is None:
                logger.debug('determined that %s is not present on destination', source_file)

            if source_folder.folder_type() == 'local':
                if source_file is not None:
                    reporter.update_compare(1)
            else:
                if dest_file is not None:
                    reporter.update_compare(1)

            for action in self.make_file_sync_actions(
                sync_type, source_file, dest_file, source_folder, dest_folder, now_millis
            ):
                yield action

    def make_file_sync_actions(
        self,
        sync_type,
        source_file,
        dest_file,
        source_folder,
        dest_folder,
        now_millis,
    ):
        """
        Yields the sequence of actions needed to sync the two files

        :param sync_type: synchronization type
        :type sync_type: str
        :param source_file: source file object
        :type source_folder: b2sdk.sync.folder.AbstractFolder
        :param dest_file: destination file object
        :type dest_file: b2sdk.sync.file.File
        :param source_folder: a source folder object
        :type source_folder: b2sdk.sync.folder.AbstractFolder
        :param dest_folder: a destination folder object
        :type dest_folder: b2sdk.sync.folder.AbstractFolder
        :param now_millis: current time in milliseconds
        :type now_millis: int
        """
        delete = self.keep_days_or_delete == self.DELETE_MODE
        keep_days = self.keep_days

        policy = POLICY_MANAGER.get_policy(
            sync_type,
            source_file,
            source_folder,
            dest_file,
            dest_folder,
            now_millis,
            delete,
            keep_days,
            self.newer_file_mode,
            self.compare_threshold,
            self.compare_version_mode,
        )
        for action in policy.get_all_actions():
            yield action
