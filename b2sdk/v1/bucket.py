######################################################################
#
# File: b2sdk/v1/bucket.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional

from .file_version import translate_single_file_version, FileVersionInfoFactory
from b2sdk import _v2 as v2
from b2sdk.utils import validate_b2_file_name


# Overridden to retain the obsolete copy_file and start_large_file methods
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

    create_file = translate_single_file_version(v2.Bucket.create_file)
    create_file_stream = translate_single_file_version(v2.Bucket.create_file_stream)
    copy = translate_single_file_version(v2.Bucket.copy)


class BucketFactory(v2.BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
