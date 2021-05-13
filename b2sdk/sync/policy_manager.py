######################################################################
#
# File: b2sdk/sync/policy_manager.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .policy import CopyAndDeletePolicy, CopyAndKeepDaysPolicy, CopyPolicy, \
    DownAndDeletePolicy, DownAndKeepDaysPolicy, DownPolicy, UpAndDeletePolicy, \
    UpAndKeepDaysPolicy, UpPolicy
from .path import AbstractSyncPath


class SyncPolicyManager(object):
    """
    Policy manager; implement a logic to get a correct policy class
    and create a policy object based on various parameters.
    """

    def __init__(self):
        self.policies = {}  # dict<,>

    def get_policy(
        self,
        sync_type,
        source_path: AbstractSyncPath,
        source_folder,
        dest_path: AbstractSyncPath,
        dest_folder,
        now_millis,
        delete,
        keep_days,
        newer_file_mode,
        compare_threshold,
        compare_version_mode,
        encryption_settings_provider,
    ):
        """
        Return a policy object.

        :param str sync_type: synchronization type
        :param b2sdk.v1.AbstractSyncPath source_path: source file
        :param str source_folder: a source folder path
        :param b2sdk.v1.AbstractSyncPath dest_path: destination file
        :param str dest_folder: a destination folder path
        :param int now_millis: current time in milliseconds
        :param bool delete: delete policy
        :param int keep_days: keep for days policy
        :param b2sdk.v1.NewerFileSyncMode newer_file_mode: setting which determines handling for destination files newer than on the source
        :param int compare_threshold: difference between file modification time or file size
        :param b2sdk.v1.CompareVersionMode compare_version_mode: setting which determines how to compare source and destination files
        :param b2sdk.v1.AbstractSyncEncryptionSettingsProvider encryption_settings_provider: an object which decides which encryption to use (if any)
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
            'invalid sync type: %s, keep_days: %s, delete: %s' % (
                sync_type,
                keep_days,
                delete,
            )
        )


POLICY_MANAGER = SyncPolicyManager()
