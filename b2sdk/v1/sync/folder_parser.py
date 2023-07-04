######################################################################
#
# File: b2sdk/v1/sync/folder_parser.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import v2
from .. import exception

from .folder import LocalFolder, B2Folder


# Override to use v1 version of "LocalFolder" and "B2Folder" and raise old style CommandError
def parse_sync_folder(folder_name, api):
    try:
        return v2.parse_sync_folder(folder_name, api, LocalFolder, B2Folder)
    except exception.InvalidArgument as ex:
        raise exception.CommandError(ex.message)
