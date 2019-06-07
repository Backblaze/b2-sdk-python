######################################################################
#
# File: b2sdk/sync/policy_manager.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .policy import DownAndDeletePolicy, DownAndKeepDaysPolicy, DownPolicy
from .policy import UpAndDeletePolicy, UpAndKeepDaysPolicy, UpPolicy


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
            source_file,
            source_folder,
            dest_file,
            dest_folder,
            now_millis,
            delete,
            keep_days,
            newer_file_mode,
            compare_threshold,
            compare_version_mode,
    ):
        """
        Return a policy object.

        :param sync_type: synchronization type
        :type sync_type: str
        :param source_file: source file name
        :type source_file: str
        :param source_folder: a source folder path
        :type source_folder: str
        :param dest_file: destination file name
        :type dest_file: str
        :param dest_folder: a destination folder path
        :type dest_folder: str
        :param now_millis: current time in milliseconds
        :type now_millis: int
        :param delete: delete policy
        :type delete: bool
        :param keep_days: keep for days policy
        :type keep_days: int
        :return: a policy object
        """
        policy_class = self.get_policy_class(sync_type, delete, keep_days)
        return policy_class(
            source_file,
            source_folder,
            dest_file,
            dest_folder,
            now_millis,
            keep_days,
            newer_file_mode,
            compare_threshold,
            compare_version_mode,
        )

    def get_policy_class(self, sync_type, delete, keep_days):
        """
        Get policy class by a given sync type.

        :param sync_type: synchronization type
        :type sync_type: str
        :param args: an object which holds command line arguments
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
        assert False, 'invalid sync type: %s, keep_days: %s, delete: %s' % (
            sync_type,
            keep_days,
            delete,
        )


POLICY_MANAGER = SyncPolicyManager()
