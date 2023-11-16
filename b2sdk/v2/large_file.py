######################################################################
#
# File: b2sdk/v2/large_file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import _v3 as v3


class UnfinishedLargeFile(v3.UnfinishedLargeFile):
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.file_id: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    :ivar str ~.account_id: account ID
    :ivar str ~.bucket_id: bucket ID
    :ivar str ~.content_type: :rfc:`822` content type, for example ``"application/octet-stream"``
    :ivar dict ~.file_info: file info dict
    """

    # In v3, cache_control is a property.
    # We set this to None so that it can be assigned to in __init__.
    cache_control = None

    def __init__(self, file_dict):
        """
        Initialize from one file returned by ``b2_start_large_file`` or ``b2_list_unfinished_large_files``.
        """
        super().__init__(file_dict)
        self.cache_control = (file_dict['fileInfo'] or {}).get('b2-cache-control')


class LargeFileServices(v3.LargeFileServices):
    UNFINISHED_LARGE_FILE_CLASS = staticmethod(UnfinishedLargeFile)
