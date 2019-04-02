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
    Policy manager, implements a logic to get a correct policy class
    and create a policy object based on various parameters
    """

    def __init__(self):
        self.policies = {}  # dict<,>

    def get_policy(
        self, sync_type, source_file, source_folder, dest_file, dest_folder, now_millis, args
    ):
        """
        Return policy object

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
        :param args: an object which holds command line arguments
        :return: a policy object
        """
        policy_class = self.get_policy_class(sync_type, args)
        return policy_class(source_file, source_folder, dest_file, dest_folder, now_millis, args)

    def get_policy_class(self, sync_type, args):
        """
        Get policy class by a given sync type

        :param sync_type: synchronization type
        :type sync_type: str
        :param args: an object which holds command line arguments
        :return: a policy class
        """
        if sync_type == 'local-to-b2':
            if args.delete:
                return UpAndDeletePolicy
            elif args.keepDays:
                return UpAndKeepDaysPolicy
            else:
                return UpPolicy
        elif sync_type == 'b2-to-local':
            if args.delete:
                return DownAndDeletePolicy
            elif args.keepDays:
                return DownAndKeepDaysPolicy
            else:
                return DownPolicy
        assert False, 'invalid sync type: %s, args: %s' % (sync_type, str(args))


POLICY_MANAGER = SyncPolicyManager()
