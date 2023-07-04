######################################################################
#
# File: b2sdk/v1/sync/sync.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import v2
from b2sdk.v2 import exception as v2_exception
from .file_to_path_translator import make_files_from_paths, make_paths_from_files
from .scan_policies import DEFAULT_SCAN_MANAGER, wrap_if_necessary as scan_wrap_if_necessary
from .encryption_provider import wrap_if_necessary as encryption_wrap_if_necessary
from ..exception import DestFileNewer


# Override to change "policies_manager" default argument
def zip_folders(folder_a, folder_b, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
    return v2.zip_folders(
        folder_a, folder_b, reporter, policies_manager=scan_wrap_if_necessary(policies_manager)
    )


# Override to change "policies_manager" default arguments
# and to wrap encryption_settings_providers in argument name translators
class Synchronizer(v2.Synchronizer):
    def __init__(
        self,
        max_workers,
        policies_manager=DEFAULT_SCAN_MANAGER,
        dry_run=False,
        allow_empty_source=False,
        newer_file_mode=v2.NewerFileSyncMode.RAISE_ERROR,
        keep_days_or_delete=v2.KeepOrDeleteMode.NO_DELETE,
        compare_version_mode=v2.CompareVersionMode.MODTIME,
        compare_threshold=None,
        keep_days=None,
        sync_policy_manager: v2.SyncPolicyManager = v2.POLICY_MANAGER,
    ):
        super().__init__(
            max_workers,
            scan_wrap_if_necessary(policies_manager),
            dry_run,
            allow_empty_source,
            newer_file_mode,
            keep_days_or_delete,
            compare_version_mode,
            compare_threshold,
            keep_days,
            sync_policy_manager,
        )

    def make_folder_sync_actions(
        self,
        source_folder,
        dest_folder,
        now_millis,
        reporter,
        policies_manager=DEFAULT_SCAN_MANAGER,
        encryption_settings_provider=v2.SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    ):
        return super()._make_folder_sync_actions(
            source_folder, dest_folder, now_millis, reporter,
            scan_wrap_if_necessary(policies_manager),
            encryption_wrap_if_necessary(encryption_settings_provider)
        )

    # override to retain a public method
    def make_file_sync_actions(
        self,
        sync_type,
        source_file,
        dest_file,
        source_folder,
        dest_folder,
        now_millis,
        encryption_settings_provider: v2.AbstractSyncEncryptionSettingsProvider = v2.
        SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
    ):
        """
        Yields the sequence of actions needed to sync the two files

        :param str sync_type: synchronization type
        :param b2sdk.v1.File source_file: source file object
        :param b2sdk.v1.File dest_file: destination file object
        :param b2sdk.v1.AbstractFolder source_folder: a source folder object
        :param b2sdk.v1.AbstractFolder dest_folder: a destination folder object
        :param int now_millis: current time in milliseconds
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: encryption setting provider
        """
        dest_path, source_path = make_paths_from_files(dest_file, source_file, sync_type)
        return self._make_file_sync_actions(
            sync_type,
            source_path,
            dest_path,
            source_folder,
            dest_folder,
            now_millis,
            encryption_wrap_if_necessary(encryption_settings_provider),
        )

    # override to raise old style DestFileNewer exceptions
    def _make_file_sync_actions(
        self,
        sync_type,
        source_path,
        dest_path,
        source_folder,
        dest_folder,
        now_millis,
        encryption_settings_provider: v2.AbstractSyncEncryptionSettingsProvider = v2.
        SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER,
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
        try:
            yield from super()._make_file_sync_actions(
                sync_type,
                source_path,
                dest_path,
                source_folder,
                dest_folder,
                now_millis,
                encryption_wrap_if_necessary(encryption_settings_provider),
            )
        except v2_exception.DestFileNewer as ex:
            dest_file, source_file = make_files_from_paths(ex.dest_path, ex.source_path, sync_type)
            raise DestFileNewer(dest_file, source_file, ex.dest_prefix, ex.source_prefix)
