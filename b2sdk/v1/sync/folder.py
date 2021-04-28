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
from .scan_policies import DEFAULT_SCAN_MANAGER
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


class B2Folder(v2.B2Folder, AbstractFolder):
    pass


# "policies_manager" default argument and translate nice errors to old style Exceptions and CommandError
class LocalFolder(v2.LocalFolder, AbstractFolder):
    @translate_errors
    def ensure_present(self):
        return super().ensure_present()

    @translate_errors
    def ensure_non_empty(self):
        return super().ensure_non_empty()
