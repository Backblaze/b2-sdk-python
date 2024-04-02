######################################################################
#
# File: b2sdk/v1/bucket.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from contextlib import suppress
from typing import overload

from .download_dest import AbstractDownloadDestination
from .file_metadata import FileMetadata
from .file_version import FileVersionInfo, FileVersionInfoFactory, file_version_info_from_download_version
from b2sdk import v2
from b2sdk._internal.utils import validate_b2_file_name
from b2sdk._internal.raw_api import LifecycleRule


# Overridden to retain the obsolete copy_file and start_large_file methods
# and to retain old style FILE_VERSION_FACTORY attribute
# and to retain old style download_file_by_name signature
# and to retain old style download_file_by_id signature (allowing for the new one as well)
# and to retain old style get_file_info_by_name return type
# and to to adjust to old style B2Api.get_file_info return type
# and to retain old style update return type
class Bucket(v2.Bucket):
    FILE_VERSION_FACTORY = staticmethod(FileVersionInfoFactory)

    def copy_file(
        self,
        file_id,
        new_file_name,
        bytes_range=None,
        metadata_directive=None,
        content_type=None,
        file_info=None,
        destination_encryption: v2.EncryptionSetting | None = None,
        source_encryption: v2.EncryptionSetting | None = None,
        file_retention: v2.FileRetentionSetting | None = None,
        legal_hold: v2.LegalHold | None = None,
        cache_control: str | None = None,
    ):
        """
        Creates a new file in this bucket by (server-side) copying from an existing file.

        :param str file_id: file ID of existing file
        :param str new_file_name: file name of the new file
        :param tuple[int,int],None bytes_range: start and end offsets (**inclusive!**), default is the entire file
        :param b2sdk.v1.MetadataDirectiveMode,None metadata_directive: default is :py:attr:`b2sdk.v1.MetadataDirectiveMode.COPY`
        :param str,None content_type: content_type for the new file if metadata_directive is set to :py:attr:`b2sdk.v1.MetadataDirectiveMode.REPLACE`, default will copy the content_type of old file
        :param dict,None file_info: file_info for the new file if metadata_directive is set to :py:attr:`b2sdk.v1.MetadataDirectiveMode.REPLACE`, default will copy the file_info of old file
        :param b2sdk.v1.EncryptionSetting destination_encryption: encryption settings for the destination
                (``None`` if unknown)
        :param b2sdk.v1.EncryptionSetting source_encryption: encryption settings for the source
                (``None`` if unknown)
        :param b2sdk.v1.FileRetentionSetting file_retention: retention setting for the new file
        :param bool legal_hold: legalHold setting for the new file
        :param str cache_control: cache control setting for the new file. Syntax based on the section 14.9 of RC 2616. Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        """
        file_info = file_info or {}
        if cache_control is not None:
            file_info['b2-cache-control'] = cache_control
        return self.api.session.copy_file(
            file_id,
            new_file_name,
            bytes_range,
            metadata_directive,
            content_type,
            file_info,
            self.id_,
            destination_server_side_encryption=destination_encryption,
            source_server_side_encryption=source_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def start_large_file(
        self,
        file_name,
        content_type=None,
        file_info=None,
        file_retention: v2.FileRetentionSetting | None = None,
        legal_hold: v2.LegalHold | None = None,
        cache_control: str | None = None,
    ):
        """
        Start a large file transfer.

        :param str file_name: a file name
        :param str,None content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v1.FileRetentionSetting file_retention: retention setting for the new file
        :param bool legal_hold: legalHold setting for the new file
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616. Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        """
        file_info = file_info or {}
        if cache_control is not None:
            file_info['b2-cache-control'] = cache_control
        validate_b2_file_name(file_name)
        return self.api.services.large_file.start_large_file(
            self.id_,
            file_name,
            content_type=content_type,
            file_info=file_info,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def download_file_by_name(
        self,
        file_name: str,
        download_dest: AbstractDownloadDestination,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ):
        """
        Download a file by name.

        .. seealso::

            :ref:`Synchronizer <sync>`, a *high-performance* utility that synchronizes a local folder with a Bucket.

        :param str file_name: a file name
        :param download_dest: an instance of the one of the following classes: \
        :class:`~b2sdk.v1.DownloadDestLocalFile`,\
        :class:`~b2sdk.v1.DownloadDestBytes`,\
        :class:`~b2sdk.v1.PreSeekedDownloadDest`,\
        or any sub class of :class:`~b2sdk.v1.AbstractDownloadDestination`
        :param progress_listener: a progress listener object to use, or ``None`` to not track progress
        :param range_: two integer values, start and end offsets
        :param encryption: encryption settings (``None`` if unknown)
        """
        downloaded_file = super().download_file_by_name(
            file_name=file_name,
            progress_listener=progress_listener,
            range_=range_,
            encryption=encryption,
        )
        try:
            return download_file_and_return_info_dict(downloaded_file, download_dest, range_)
        except ValueError as ex:
            if ex.args == ('no strategy suitable for download was found!',):
                raise AssertionError('no strategy suitable for download was found!')
            raise

    @overload
    def download_file_by_id(
        self,
        file_id: str,
        download_dest: AbstractDownloadDestination = None,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ) -> dict:
        ...

    @overload
    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ) -> v2.DownloadedFile:
        ...

    def download_file_by_id(
        self,
        file_id: str,
        download_dest: AbstractDownloadDestination | None = None,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ):
        """
        Download a file by ID.

        .. note::
          download_file_by_id actually belongs in :py:class:`b2sdk.v1.B2Api`, not in :py:class:`b2sdk.v1.Bucket`; we just provide a convenient redirect here

        :param file_id: a file ID
        :param download_dest: an instance of the one of the following classes: \
        :class:`~b2sdk.v1.DownloadDestLocalFile`,\
        :class:`~b2sdk.v1.DownloadDestBytes`,\
        :class:`~b2sdk.v1.PreSeekedDownloadDest`,\
        or any sub class of :class:`~b2sdk.v1.AbstractDownloadDestination`
        :param progress_listener: a progress listener object to use, or ``None`` to not report progress
        :param range_: two integer values, start and end offsets
        :param encryption: encryption settings (``None`` if unknown)
        """
        return self.api.download_file_by_id(
            file_id,
            download_dest,
            progress_listener,
            range_=range_,
            encryption=encryption,
        )

    def get_file_info_by_name(self, file_name: str) -> FileVersionInfo:
        return file_version_info_from_download_version(super().get_file_info_by_name(file_name))

    def get_file_info_by_id(self, file_id: str) -> FileVersionInfo:
        """
        Gets a file version's by ID.

        :param str file_id: the id of the file.
        """
        return self.api.file_version_factory.from_api_response(self.api.get_file_info(file_id))

    def update(
        self,
        bucket_type: str | None = None,
        bucket_info: dict | None = None,
        cors_rules: dict | None = None,
        lifecycle_rules: list[LifecycleRule] | None = None,
        if_revision_is: int | None = None,
        default_server_side_encryption: v2.EncryptionSetting | None = None,
        default_retention: v2.BucketRetentionSetting | None = None,
        is_file_lock_enabled: bool | None = None,
        **kwargs
    ):
        """
        Update various bucket parameters.

        :param bucket_type: a bucket type, e.g. ``allPrivate`` or ``allPublic``
        :param bucket_info: an info to store with a bucket
        :param cors_rules: CORS rules to store with a bucket
        :param lifecycle_rules: lifecycle rules of the bucket
        :param if_revision_is: revision number, update the info **only if** *revision* equals to *if_revision_is*
        :param default_server_side_encryption: default server side encryption settings (``None`` if unknown)
        :param default_retention: bucket default retention setting
        :param bool is_file_lock_enabled: specifies whether bucket should get File Lock-enabled
        """
        # allow common tests to execute without hitting attributeerror

        with suppress(KeyError):
            del kwargs['replication']
        self.replication = None
        assert not kwargs  # after we get rid of everything we don't support in this apiver, this should be empty

        account_id = self.api.account_info.get_account_id()
        return self.api.session.update_bucket(
            account_id,
            self.id_,
            bucket_type=bucket_type,
            bucket_info=bucket_info,
            cors_rules=cors_rules,
            lifecycle_rules=lifecycle_rules,
            if_revision_is=if_revision_is,
            default_server_side_encryption=default_server_side_encryption,
            default_retention=default_retention,
            is_file_lock_enabled=is_file_lock_enabled,
        )

    def ls(
        self,
        folder_to_list: str = '',
        show_versions: bool = False,
        recursive: bool = False,
        fetch_count: int | None = 10000,
        **kwargs
    ):
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
        :param show_versions: when ``True`` returns info about all versions of a file,
                              when ``False``, just returns info about the most recent versions
        :param recursive: if ``True``, list folders recursively
        :param fetch_count: how many entries to return or ``None`` to use the default. Acceptable values: 1 - 10000
        :rtype: generator[tuple[b2sdk.v1.FileVersionInfo, str]]
        :returns: generator of (file_version, folder_name) tuples

        .. note::
            In case of `recursive=True`, folder_name is not returned.
        """
        return super().ls(folder_to_list, not show_versions, recursive, fetch_count, **kwargs)


def download_file_and_return_info_dict(
    downloaded_file: v2.DownloadedFile, download_dest: AbstractDownloadDestination,
    range_: tuple[int, int] | None
):
    with download_dest.make_file_context(
        file_id=downloaded_file.download_version.id_,
        file_name=downloaded_file.download_version.file_name,
        content_length=downloaded_file.download_version.size,
        content_type=downloaded_file.download_version.content_type,
        content_sha1=downloaded_file.download_version.content_sha1,
        file_info=downloaded_file.download_version.file_info,
        mod_time_millis=downloaded_file.download_version.mod_time_millis,
        range_=range_,
    ) as file:
        downloaded_file.save(file)
        return FileMetadata.from_download_version(downloaded_file.download_version).as_info_dict()


class BucketFactory(v2.BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
