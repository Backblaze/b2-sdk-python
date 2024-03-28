######################################################################
#
# File: b2sdk/_internal/scan/folder_parser.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from .exception import InvalidArgument
from .folder import B2Folder, LocalFolder


def parse_folder(folder_name, api, local_folder_class=LocalFolder, b2_folder_class=B2Folder):
    """
    Take either a local path, or a B2 path, and returns a Folder
    object for it.

    B2 paths look like: b2://bucketName/path/name.  The '//' is optional.

    Anything else is treated like a local folder.

    :param folder_name: a name of the folder, either local or remote
    :type folder_name: str
    :param api: an API object
    :type api: :class:`~b2sdk.v2.B2Api`
    :param local_folder_class: class to handle local folders
    :type local_folder_class: `~b2sdk.v2.AbstractFolder`
    :param b2_folder_class: class to handle B2 folders
    :type b2_folder_class: `~b2sdk.v2.AbstractFolder`
    """
    if folder_name.startswith('b2://'):
        return _parse_bucket_and_folder(folder_name[5:], api, b2_folder_class)
    elif folder_name.startswith('b2:') and folder_name[3].isalnum():
        return _parse_bucket_and_folder(folder_name[3:], api, b2_folder_class)
    else:
        return local_folder_class(folder_name)


def _parse_bucket_and_folder(bucket_and_path, api, b2_folder_class):
    """
    Turn 'my-bucket/foo' into B2Folder(my-bucket, foo).
    """
    if '//' in bucket_and_path:
        raise InvalidArgument('folder_name', "'//' not allowed in path names")
    if '/' not in bucket_and_path:
        bucket_name = bucket_and_path
        folder_name = ''
    else:
        (bucket_name, folder_name) = bucket_and_path.split('/', 1)
    if folder_name.endswith('/'):
        folder_name = folder_name[:-1]
    return b2_folder_class(bucket_name, folder_name, api)
