######################################################################
#
# File: b2sdk/v0/sync.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging

from b2sdk.v1.exception import CommandError
from b2sdk.v1.exception import DestFileNewer as DestFileNewerV1
from b2sdk.v1 import trace_call
from .exception import DestFileNewer
from b2sdk.v1.exception import InvalidArgument, IncompleteSync
from b2sdk.v1 import NewerFileSyncMode, CompareVersionMode
from b2sdk.v1 import KeepOrDeleteMode
from b2sdk.v1 import DEFAULT_SCAN_MANAGER
from b2sdk.v1 import SyncReport
from b2sdk.v1 import Synchronizer as SynchronizerV1
from b2sdk.v1 import AbstractSyncEncryptionSettingsProvider, SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER

logger = logging.getLogger(__name__)


class Synchronizer(SynchronizerV1):
    """
    This is wrapper for newer version and will return the v0 style exceptions
    """

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except InvalidArgument as e:
            raise CommandError(f'--{e.parameter_name} {e.message}')

    def _make_file_sync_actions(self, *args, **kwargs):
        try:
            yield from super()._make_file_sync_actions(*args, **kwargs)
        except DestFileNewerV1 as e:
            raise DestFileNewer(e.dest_file, e.source_file, e.dest_prefix, e.source_prefix)

    def sync_folders(self, *args, **kwargs):
        try:
            super().sync_folders(*args, **kwargs)
        except InvalidArgument as e:
            raise CommandError(f'--{e.parameter_name} {e.message}')
        except IncompleteSync as e:
            raise CommandError(str(e))


def get_synchronizer_from_args(
    args,
    max_workers,
    policies_manager=DEFAULT_SCAN_MANAGER,
    dry_run=False,
    allow_empty_source=False,
):
    if args.replaceNewer and args.skipNewer:
        raise CommandError('--skipNewer and --replaceNewer are incompatible')
    elif args.replaceNewer:
        newer_file_mode = NewerFileSyncMode.REPLACE
    elif args.skipNewer:
        newer_file_mode = NewerFileSyncMode.SKIP
    else:
        newer_file_mode = NewerFileSyncMode.RAISE_ERROR

    if args.delete and (args.keepDays is not None):
        raise CommandError('--delete and --keepDays are incompatible')

    if args.compareVersions == 'none':
        compare_version_mode = CompareVersionMode.NONE
    elif args.compareVersions == 'modTime':
        compare_version_mode = CompareVersionMode.MODTIME
    elif args.compareVersions == 'size':
        compare_version_mode = CompareVersionMode.SIZE
    elif args.compareVersions is None:
        compare_version_mode = CompareVersionMode.MODTIME
    else:
        raise CommandError('Invalid option for --compareVersions')
    compare_threshold = args.compareThreshold

    keep_days = None

    if args.delete:
        keep_days_or_delete = KeepOrDeleteMode.DELETE
    elif args.keepDays:
        keep_days_or_delete = KeepOrDeleteMode.KEEP_BEFORE_DELETE
        keep_days = args.keepDays
    else:
        keep_days_or_delete = KeepOrDeleteMode.NO_DELETE

    return Synchronizer(
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
    encryption_settings_provider:
    AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
):
    """
    This is deprecated. Use the new Synchronizer class.
    Yields a sequence of actions that will sync the destination
    folder to the source folder.

    :param source_folder: source folder object
    :type source_folder: b2sdk._internal.scan.folder.AbstractFolder
    :param dest_folder: destination folder object
    :type dest_folder: b2sdk._internal.scan.folder.AbstractFolder
    :param args: an object which holds command line arguments
    :param now_millis: current time in milliseconds
    :type now_millis: int
    :param reporter: reporter object
    :param policies_manager: policies manager object
    :param encryption_settings_provider: encryption settings provider
    :type encryption_settings_provider: AbstractSyncEncryptionSettingsProvider
    """
    synchronizer = get_synchronizer_from_args(
        args,
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
            encryption_settings_provider=encryption_settings_provider
        )
    except InvalidArgument as e:
        raise CommandError(f'--{e.parameter_name} {e.message}')


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
    encryption_settings_provider:
    AbstractSyncEncryptionSettingsProvider = SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
):
    """
    This is deprecated. Use the new Synchronizer class.

    source is also in the destination.  Deletes any file versions
    in the destination older than history_days.

    :param source_folder: source folder object
    :type source_folder: b2sdk._internal.scan.folder.AbstractFolder
    :param dest_folder: destination folder object
    :type dest_folder: b2sdk._internal.scan.folder.AbstractFolder
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
    :param encryption_settings_provider: encryption settings provider
    :type encryption_settings_provider: AbstractSyncEncryptionSettingsProvider
    """
    synchronizer = get_synchronizer_from_args(
        args,
        max_workers,
        policies_manager=policies_manager,
        dry_run=dry_run,
        allow_empty_source=allow_empty_source,
    )
    with SyncReport(stdout, no_progress) as reporter:
        synchronizer.sync_folders(
            source_folder,
            dest_folder,
            now_millis,
            reporter,
            encryption_settings_provider=encryption_settings_provider
        )
