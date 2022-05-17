######################################################################
#
# File: b2sdk/scan/scan.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional, Tuple

from .folder import AbstractFolder
from .path import AbstractPath
from .policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from .report import Report


def zip_folders(
    folder_a: AbstractFolder,
    folder_b: AbstractFolder,
    reporter: Report,
    policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER,
) -> Tuple[Optional[AbstractPath], Optional[AbstractPath]]:
    """
    Iterate over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.

    :param b2sdk.scan.folder.AbstractFolder folder_a: first folder object.
    :param b2sdk.scan.folder.AbstractFolder folder_b: second folder object.
    :param reporter: reporter object
    :param policies_manager: policies manager object
    :return: yields two element tuples
    """

    iter_a = folder_a.all_files(reporter, policies_manager)
    iter_b = folder_b.all_files(reporter)

    current_a = next(iter_a, None)
    current_b = next(iter_b, None)

    while current_a is not None or current_b is not None:
        if current_a is None:
            yield (None, current_b)
            current_b = next(iter_b, None)
        elif current_b is None:
            yield (current_a, None)
            current_a = next(iter_a, None)
        elif current_a.relative_path < current_b.relative_path:
            yield (current_a, None)
            current_a = next(iter_a, None)
        elif current_b.relative_path < current_a.relative_path:
            yield (None, current_b)
            current_b = next(iter_b, None)
        else:
            assert current_a.relative_path == current_b.relative_path
            yield (current_a, current_b)
            current_a = next(iter_a, None)
            current_b = next(iter_b, None)
