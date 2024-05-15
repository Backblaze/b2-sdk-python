######################################################################
#
# File: b2sdk/v2/bucket.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import typing

from b2sdk import _v3 as v3
from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from b2sdk.v2._compat import _file_infos_rename
from b2sdk._internal.http_constants import LIST_FILE_NAMES_MAX_LIMIT
from .exception import BucketIdNotFound
from .file_version import FileVersionFactory

if typing.TYPE_CHECKING:
    from b2sdk._internal.utils import Sha1HexDigest
    from b2sdk._internal.filter import Filter
    from .file_version import FileVersion


# Overridden to raise old style BucketIdNotFound exception
class Bucket(v3.Bucket):

    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionFactory)

    def get_fresh_state(self) -> Bucket:
        try:
            return super().get_fresh_state()
        except v3BucketIdNotFound as e:
            raise BucketIdNotFound(e.bucket_id)

    @_file_infos_rename
    def upload_bytes(
        self,
        data_bytes,
        file_name,
        content_type=None,
        file_info: dict | None = None,
        progress_listener=None,
        encryption: v3.EncryptionSetting | None = None,
        file_retention: v3.FileRetentionSetting | None = None,
        legal_hold: v3.LegalHold | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs
    ):
        return super().upload_bytes(
            data_bytes,
            file_name,
            content_type,
            file_info,
            progress_listener,
            encryption,
            file_retention,
            legal_hold,
            large_file_sha1,
            custom_upload_timestamp,
            cache_control,
            *args,
            **kwargs,
        )

    @_file_infos_rename
    def upload_local_file(
        self,
        local_file,
        file_name,
        content_type=None,
        file_info: dict | None = None,
        sha1_sum=None,
        min_part_size=None,
        progress_listener=None,
        encryption: v3.EncryptionSetting | None = None,
        file_retention: v3.FileRetentionSetting | None = None,
        legal_hold: v3.LegalHold | None = None,
        upload_mode: v3.UploadMode = v3.UploadMode.FULL,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs
    ):
        return super().upload_local_file(
            local_file,
            file_name,
            content_type,
            file_info,
            sha1_sum,
            min_part_size,
            progress_listener,
            encryption,
            file_retention,
            legal_hold,
            upload_mode,
            custom_upload_timestamp,
            cache_control,
            *args,
            **kwargs,
        )

    def ls(
        self,
        folder_to_list: str = '',
        latest_only: bool = True,
        recursive: bool = False,
        fetch_count: int | None = LIST_FILE_NAMES_MAX_LIMIT,
        with_wildcard: bool = False,
        filters: typing.Sequence[Filter] = (),
        folder_to_list_can_be_a_file: bool = False,
        **kwargs
    ) -> typing.Iterable[tuple[FileVersion, str]]:
        """
        Pretend that folders exist and yields the information about the files in a folder.

        B2 has a flat namespace for the files in a bucket, but there is a convention
        of using "/" as if there were folders.  This method searches through the
        flat namespace to find the files and "folders" that live within a given
        folder.

        When the `recursive` flag is set, lists all of the files in the given
        folder, and all of its sub-folders.

        :param folder_to_list: the name of the folder to list; must not start with "/".
                               Empty string means top-level folder
        :param latest_only: when ``False`` returns info about all versions of a file,
                            when ``True``, just returns info about the most recent versions
        :param recursive: if ``True``, list folders recursively
        :param fetch_count: how many entries to list per API call or ``None`` to use the default. Acceptable values: 1 - 10000
        :param with_wildcard: Accepts "*", "?", "[]" and "[!]" in folder_to_list, similarly to what shell does.
                              As of 1.19.0 it can only be enabled when recursive is also enabled.
                              Also, in this mode, folder_to_list is considered to be a filename or a pattern.
        :param filters: list of filters to apply to the files returned by the server.
        :param folder_to_list_can_be_a_file: if ``True``, folder_to_list can be a file, not just a folder
                                             This enabled default behavior of b2sdk.v3.Bucket.ls, in which for all
                                             paths that do not end with '/', first we try to check if file with this
                                             exact name exists, and only if it does not then we try to list files with
                                             this prefix.
        :rtype: generator[tuple[b2sdk.v2.FileVersion, str]]
        :returns: generator of (file_version, folder_name) tuples

        .. note::
            In case of `recursive=True`, folder_name is not returned.
        """
        if not folder_to_list_can_be_a_file and folder_to_list and not folder_to_list.endswith(
            '/'
        ) and not with_wildcard:
            folder_to_list += '/'
        yield from super().ls(
            path=folder_to_list,
            latest_only=latest_only,
            recursive=recursive,
            fetch_count=fetch_count,
            with_wildcard=with_wildcard,
            filters=filters,
            **kwargs
        )


# Overridden to use old style Bucket
class BucketFactory(v3.BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
