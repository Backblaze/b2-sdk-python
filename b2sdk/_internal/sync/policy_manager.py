######################################################################
#
# File: b2sdk/_internal/sync/policy_manager.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from ..scan.folder import AbstractFolder
from ..scan.path import AbstractPath
from ..transfer.outbound.upload_source import UploadMode
from .encryption_provider import AbstractSyncEncryptionSettingsProvider
from .policy import (
    AbstractFileSyncPolicy,
    CompareVersionMode,
    CopyAndDeletePolicy,
    CopyAndKeepDaysPolicy,
    CopyPolicy,
    DownAndDeletePolicy,
    DownAndKeepDaysPolicy,
    DownPolicy,
    NewerFileSyncMode,
    UpAndDeletePolicy,
    UpAndKeepDaysPolicy,
    UpPolicy,
)


class SyncPolicyManager:
    """
    Policy manager; implement a logic to get a correct policy class
    and create a policy object based on various parameters.
    """

    def __init__(self):
        self.policies = {}  # dict<,>

    def get_policy(
        self,
        sync_type: str,
        source_path: AbstractPath | None,
        source_folder: AbstractFolder,
        dest_path: AbstractPath | None,
        dest_folder: AbstractFolder,
        now_millis: int,
        delete: bool,
        keep_days: int,
        newer_file_mode: NewerFileSyncMode,
        compare_threshold: int,
        compare_version_mode: CompareVersionMode,
        encryption_settings_provider: AbstractSyncEncryptionSettingsProvider,
        upload_mode: UploadMode,
        absolute_minimum_part_size: int,
    ) -> AbstractFileSyncPolicy:
        """
        Return a policy object.

        :param sync_type: synchronization type
        :param source_path: source file
        :param source_folder: a source folder path
        :param dest_path: destination file
        :param dest_folder: a destination folder path
        :param now_millis: current time in milliseconds
        :param delete: delete policy
        :param keep_days: keep for days policy
        :param newer_file_mode: setting which determines handling for destination files newer than on the source
        :param compare_threshold: difference between file modification time or file size
        :param compare_version_mode: setting which determines how to compare source and destination files
        :param encryption_settings_provider: an object which decides which encryption to use (if any)
        :param upload_mode: determines how file uploads are handled
        :param absolute_minimum_part_size: minimum file part size for large files
        :return: a policy object
        """
        policy_class = self.get_policy_class(sync_type, delete, keep_days)
        return policy_class(
            source_path,
            source_folder,
            dest_path,
            dest_folder,
            now_millis,
            keep_days,
            newer_file_mode,
            compare_threshold,
            compare_version_mode,
            encryption_settings_provider,
            upload_mode,
            absolute_minimum_part_size,
        )

    def get_policy_class(self, sync_type, delete, keep_days):
        """
        Get policy class by a given sync type.

        :param str sync_type: synchronization type
        :param bool delete: if True, delete files and update from source
        :param int keep_days: keep for `keep_days` before delete
        :return: a policy class
        """
        if sync_type == 'local-to-b2':
            if delete:
                return UpAndDeletePolicy
            elif keep_days:
                return UpAndKeepDaysPolicy
            else:
                return UpPolicy
        elif sync_type == 'b2-to-local':
            if delete:
                return DownAndDeletePolicy
            elif keep_days:
                return DownAndKeepDaysPolicy
            else:
                return DownPolicy
        elif sync_type == 'b2-to-b2':
            if delete:
                return CopyAndDeletePolicy
            elif keep_days:
                return CopyAndKeepDaysPolicy
            else:
                return CopyPolicy
        raise NotImplementedError(
            f'invalid sync type: {sync_type}, keep_days: {keep_days}, delete: {delete}'
        )


POLICY_MANAGER = SyncPolicyManager()
