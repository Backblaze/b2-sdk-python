######################################################################
#
# File: b2sdk/v1/sync/folder.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import abstractmethod
import functools

from b2sdk import v2
from .scan_policies import DEFAULT_SCAN_MANAGER, wrap_if_necessary
from .. import exception


def translate_errors(func):
    @functools.wraps(func)
    def wrapper(*a, **kw):
        try:
            return func(*a, **kw)
        except exception.NotADirectory as ex:
            raise Exception(f'{ex.path} is not a directory')
        except exception.UnableToCreateDirectory as ex:
            raise Exception(f'unable to create directory {ex.path}')
        except exception.EmptyDirectory as ex:
            raise exception.CommandError(
                f'Directory {ex.path} is empty.  Use --allowEmptySource to sync anyway.'
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

    def get_file_versions(self):
        for file_version, _ in self.bucket.ls(
            self.folder_name,
            show_versions=True,
            recursive=True,
        ):
            yield file_version


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
