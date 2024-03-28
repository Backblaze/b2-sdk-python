######################################################################
#
# File: b2sdk/_internal/large_file/services.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.file_lock import FileRetentionSetting, LegalHold
from b2sdk._internal.file_version import FileIdAndName
from b2sdk._internal.large_file.part import PartFactory
from b2sdk._internal.large_file.unfinished_large_file import UnfinishedLargeFile


class LargeFileServices:

    UNFINISHED_LARGE_FILE_CLASS = staticmethod(UnfinishedLargeFile)

    def __init__(self, services):
        self.services = services

    def list_parts(self, file_id, start_part_number=None, batch_size=None):
        """
        Generator that yields a :py:class:`b2sdk.v2.Part` for each of the parts that have been uploaded.

        :param str file_id: the ID of the large file that is not finished
        :param int start_part_number: the first part number to return; defaults to the first part
        :param int batch_size: the number of parts to fetch at a time from the server
        :rtype: generator
        """
        batch_size = batch_size or 100
        while True:
            response = self.services.session.list_parts(file_id, start_part_number, batch_size)
            for part_dict in response['parts']:
                yield PartFactory.from_list_parts_dict(part_dict)
            start_part_number = response.get('nextPartNumber')
            if start_part_number is None:
                break

    def list_unfinished_large_files(
        self, bucket_id, start_file_id=None, batch_size=None, prefix=None
    ):
        """
        A generator that yields an :py:class:`b2sdk.v2.UnfinishedLargeFile` for each
        unfinished large file in the bucket, starting at the given file, filtering by prefix.

        :param str bucket_id: bucket id
        :param str,None start_file_id: a file ID to start from or None to start from the beginning
        :param int,None batch_size: max file count
        :param str,None prefix: file name prefix filter
        :rtype: generator[b2sdk.v2.UnfinishedLargeFile]
        """
        batch_size = batch_size or 100
        while True:
            batch = self.services.session.list_unfinished_large_files(
                bucket_id, start_file_id, batch_size, prefix
            )
            for file_dict in batch['files']:
                yield self.UNFINISHED_LARGE_FILE_CLASS(file_dict)
            start_file_id = batch.get('nextFileId')
            if start_file_id is None:
                break

    def get_unfinished_large_file(self, bucket_id, large_file_id, prefix=None):
        result = list(
            self.list_unfinished_large_files(
                bucket_id, start_file_id=large_file_id, batch_size=1, prefix=prefix
            )
        )
        if not result:
            return None

        unfinished_large_file = result[0]
        if unfinished_large_file.file_id != large_file_id:
            return None

        return unfinished_large_file

    def start_large_file(
        self,
        bucket_id,
        file_name,
        content_type=None,
        file_info=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
    ):
        """
        Start a large file transfer.

        :param str file_name: a file name
        :param str,None content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param b2sdk.v2.LegalHold legal_hold: legal hold setting
        """
        return self.UNFINISHED_LARGE_FILE_CLASS(
            self.services.session.start_large_file(
                bucket_id,
                file_name,
                content_type,
                file_info,
                server_side_encryption=encryption,
                file_retention=file_retention,
                legal_hold=legal_hold,
            )
        )

    # delete/cancel
    def cancel_large_file(self, file_id: str) -> FileIdAndName:
        """
        Cancel a large file upload.
        """
        response = self.services.session.cancel_large_file(file_id)
        return FileIdAndName.from_cancel_or_delete_response(response)
