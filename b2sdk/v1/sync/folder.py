######################################################################
#
# File: b2sdk/v1/sync/folder.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod

from b2sdk import _v2 as v2
from .scan_policies import DEFAULT_SCAN_MANAGER


# Override to change "policies_manager" default argument
class AbstractFolder(v2.AbstractFolder):
    @abstractmethod
    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        pass


class LocalFolder(v2.LocalFolder, AbstractFolder):
    pass


class B2Folder(v2.B2Folder, AbstractFolder):
    pass
