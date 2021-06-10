######################################################################
#
# File: b2sdk/v1/bucket.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .download_dest import AbstractDownloadDestination
from .file_metadata import FileMetadata
from .file_version import FileVersionInfoFactory
from typing import Optional, overload, Tuple
from b2sdk import _v2 as v2
from b2sdk.utils import validate_b2_file_name


# Overridden to retain the obsolete copy_file and start_large_file methods
# and to retain old style FILE_VERSION_FACTORY attribute
# and to retain old style download_file_by_name signature
# and to retain old style download_file_by_id signature (allowing for the new one as well)
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
        destination_encryption: Optional[v2.EncryptionSetting] = None,
        source_encryption: Optional[v2.EncryptionSetting] = None,
        file_retention: Optional[v2.FileRetentionSetting] = None,
        legal_hold: Optional[v2.LegalHold] = None,
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
        """
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
        file_retention: Optional[v2.FileRetentionSetting] = None,
        legal_hold: Optional[v2.LegalHold] = None,
    ):
        """
        Start a large file transfer.

        :param str file_name: a file name
        :param str,None content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v1.FileRetentionSetting file_retention: retention setting for the new file
        :param bool legal_hold: legalHold setting for the new file
        """
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
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
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
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
    ) -> dict:
        ...

    @overload
    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
    ) -> v2.DownloadedFile:
        ...

    def download_file_by_id(
        self,
        file_id: str,
        download_dest: Optional[AbstractDownloadDestination] = None,
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
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


def download_file_and_return_info_dict(
    downloaded_file: v2.DownloadedFile, download_dest: AbstractDownloadDestination,
    range_: Optional[Tuple[int, int]]
):
    with download_dest.make_file_context(
        file_id=downloaded_file.file_version.id_,
        file_name=downloaded_file.file_version.file_name,
        content_length=downloaded_file.file_version.size,
        content_type=downloaded_file.file_version.content_type,
        content_sha1=downloaded_file.file_version.content_sha1,
        file_info=downloaded_file.file_version.file_info,
        mod_time_millis=downloaded_file.file_version.mod_time_millis,
        range_=range_,
    ) as file:
        downloaded_file.save(file)
        return FileMetadata.from_file_version(downloaded_file.file_version).as_info_dict()


class BucketFactory(v2.BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
