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
import functools

from b2sdk import _v2 as v2
from .scan_policies import DEFAULT_SCAN_MANAGER, wrap_if_necessary
from .. import exception


def translate_errors(func):
    @functools.wraps(func)
    def wrapper(*a, **kw):
        try:
            return func(*a, **kw)
        except exception.NotADirectory as ex:
            raise Exception('%s is not a directory' % (ex.path,))
        except exception.UnableToCreateDirectory as ex:
            raise Exception('unable to create directory %s' % (ex.path,))
        except exception.EmptyDirectory as ex:
            raise exception.CommandError(
                'Directory %s is empty.  Use --allowEmptySource to sync anyway.' % (ex.path,)
            )

    return wrapper


# Override to change "policies_manager" default argument
class AbstractFolder(v2.AbstractFolder):
    @abstractmethod
    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        pass


# override to retain "policies_manager" default argument,
# and wrap policies_manager
class B2Folder(v2.B2Folder, AbstractFolder):
    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        return super().all_files(reporter, wrap_if_necessary(policies_manager))


# override to retain "policies_manager" default argument,
# translate nice errors to old style Exceptions and CommandError
# and wrap policies_manager
class LocalFolder(v2.LocalFolder, AbstractFolder):
    @translate_errors
    def ensure_present(self):
        return super().ensure_present()

    @translate_errors
    def ensure_non_empty(self):
        return super().ensure_non_empty()

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        return super().all_files(reporter, wrap_if_necessary(policies_manager))
