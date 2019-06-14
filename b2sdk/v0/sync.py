######################################################################
#
# File: b2sdk/v0/sync.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging

from b2sdk.v1.exception import CommandError
from b2sdk.v1.exception import DestFileNewer as DestFileNewerV1
from b2sdk.v1 import trace_call
from .exception import DestFileNewer
from b2sdk.v1.exception import InvalidArgument, IncompleteSync
from b2sdk.v1 import AbstractFileSyncPolicy
from b2sdk.v1 import DEFAULT_SCAN_MANAGER
from b2sdk.v1 import SyncReport
from b2sdk.v1 import Synchronizer as SynchronizerV1

logger = logging.getLogger(__name__)


class Synchronizer(SynchronizerV1):
    def __init__(self, *args, **kwargs):
        try:
            super(Synchronizer, self).__init__(*args, **kwargs)
        except InvalidArgument as e:
            raise CommandError('--%s %s' % (e.field_name, e.message))

    def make_file_sync_actions(self, *args, **kwargs):
        try:
            for i in super(Synchronizer, self).make_file_sync_actions(*args, **kwargs):
                yield i
        except DestFileNewerV1 as e:
            raise DestFileNewer(e.dest_file, e.source_file, e.dest_prefix, e.source_prefix)

    def sync_folders(self, *args, **kwargs):
        try:
            super(Synchronizer, self).sync_folders(*args, **kwargs)
        except IncompleteSync as e:
            raise CommandError(e.message)


def get_synchronizer_from_args(
    args,
    no_progress,
    max_workers,
    policies_manager=DEFAULT_SCAN_MANAGER,
    dry_run=False,
    allow_empty_source=False,
):
    if args.replaceNewer and args.skipNewer:
        raise CommandError('--skipNewer and --replaceNewer are incompatible')
    elif args.replaceNewer:
        newer_file_mode = AbstractFileSyncPolicy.NEWER_FILE_REPLACE
    elif args.skipNewer:
        newer_file_mode = AbstractFileSyncPolicy.NEWER_FILE_SKIP
    else:
        newer_file_mode = None

    if args.delete and (args.keepDays is not None):
        raise CommandError('--delete and --keepDays are incompatible')

    if args.compareVersions == 'none':
        compare_version_mode = AbstractFileSyncPolicy.COMPARE_VERSION_NONE
    elif args.compareVersions == 'modTime':
        compare_version_mode = AbstractFileSyncPolicy.COMPARE_VERSION_MODTIME
    elif args.compareVersions == 'size':
        compare_version_mode = AbstractFileSyncPolicy.COMPARE_VERSION_SIZE
    else:
        compare_version_mode = args.compareVersions
    compare_threshold = args.compareThreshold

    keep_days_or_delete = None
    keep_days = None

    if args.delete:
        keep_days_or_delete = Synchronizer.DELETE_MODE
    elif args.keepDays:
        keep_days_or_delete = Synchronizer.KEEP_DAYS_MODE
        keep_days = args.keepDays

    return Synchronizer(
        no_progress,
        max_workers,
        policies_manager=policies_manager,
        dry_run=dry_run,
        allow_empty_source=allow_empty_source,
        newer_file_mode=newer_file_mode,
        keep_days_or_delete=keep_days_or_delete,
        compare_version_mode=compare_version_mode,
        compare_threshold=compare_threshold,
        keep_days=keep_days,
    )


def make_folder_sync_actions(
    source_folder,
    dest_folder,
    args,
    now_millis,
    reporter,
    policies_manager=DEFAULT_SCAN_MANAGER,
):
    """
    This is deprecated. Use the new Synchronizer class.
    Yields a sequence of actions that will sync the destination
    folder to the source folder.

    :param source_folder: source folder object
    :type source_folder: b2sdk.sync.folder.AbstractFolder
    :param dest_folder: destination folder object
    :type dest_folder: b2sdk.sync.folder.AbstractFolder
    :param args: an object which holds command line arguments
    :param now_millis: current time in milliseconds
    :type now_millis: int
    :param reporter: reporter object
    :param policies_manager: policies manager object
    """
    synchronizer = get_synchronizer_from_args(
        args,
        False,
        1,
        policies_manager=policies_manager,
        dry_run=False,
        allow_empty_source=False,
    )
    try:
        return synchronizer.make_folder_sync_actions(
            source_folder,
            dest_folder,
            now_millis,
            reporter,
            policies_manager=policies_manager,
        )
    except InvalidArgument as e:
        raise CommandError('--%s %s' % (e.field_name, e.message))


@trace_call(logger)
def sync_folders(
    source_folder,
    dest_folder,
    args,
    now_millis,
    stdout,
    no_progress,
    max_workers,
    policies_manager=DEFAULT_SCAN_MANAGER,
    dry_run=False,
    allow_empty_source=False,
):
    """
    This is deprecated. Use the new Synchronizer class.

    source is also in the destination.  Deletes any file versions
    in the destination older than history_days.

    :param source_folder: source folder object
    :type source_folder: b2sdk.sync.folder.AbstractFolder
    :param dest_folder: destination folder object
    :type dest_folder: b2sdk.sync.folder.AbstractFolder
    :param args: an object which holds command line arguments
    :param now_millis: current time in milliseconds
    :type now_millis: int
    :param stdout: standard output file object
    :param no_progress: if True, do not show progress
    :type no_progress: bool
    :param max_workers: max number of workers
    :type max_workers: int
    :param policies_manager: policies manager object
    :param dry_run:
    :type dry_run: bool
    :param allow_empty_source: if True, do not check whether source folder is empty
    :type allow_empty_source: bool
    """
    synchronizer = get_synchronizer_from_args(
        args,
        no_progress,
        max_workers,
        policies_manager=policies_manager,
        dry_run=dry_run,
        allow_empty_source=allow_empty_source,
    )
    with SyncReport(stdout, no_progress) as reporter:
        synchronizer.sync_folders(source_folder, dest_folder, now_millis, reporter)
