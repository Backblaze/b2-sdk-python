######################################################################
#
# File: b2sdk/v1/sync/sync.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v2 as v2
from .scan_policies import DEFAULT_SCAN_MANAGER


# Override to change "policies_manager" default argument
def zip_folders(folder_a, folder_b, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
    return v2.zip_folders(folder_a, folder_b, reporter, policies_manager=policies_manager)


# Override to change "policies_manager" default arguments
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
    ):
        super().__init__(
            max_workers, policies_manager, dry_run, allow_empty_source, newer_file_mode,
            keep_days_or_delete, compare_version_mode, compare_threshold, keep_days
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
        return super().make_folder_sync_actions(
            source_folder, dest_folder, now_millis, reporter, policies_manager,
            encryption_settings_provider
        )
