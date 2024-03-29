######################################################################
#
# File: b2sdk/_internal/transfer/outbound/copy_manager.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging

from b2sdk._internal.encryption.setting import EncryptionMode, EncryptionSetting
from b2sdk._internal.exception import AlreadyFailed, CopyArgumentsMismatch, SSECKeyIdMismatchInCopy
from b2sdk._internal.file_lock import FileRetentionSetting, LegalHold
from b2sdk._internal.http_constants import SSE_C_KEY_ID_FILE_INFO_KEY_NAME
from b2sdk._internal.progress import AbstractProgressListener
from b2sdk._internal.raw_api import MetadataDirectiveMode
from b2sdk._internal.transfer.transfer_manager import TransferManager
from b2sdk._internal.utils.thread_pool import ThreadPoolMixin

logger = logging.getLogger(__name__)


class CopyManager(TransferManager, ThreadPoolMixin):
    """
    Handle complex actions around server side copy to free raw_api from that responsibility.
    """

    MAX_LARGE_FILE_SIZE = 10 * 1000 * 1000 * 1000 * 1000  # 10 TB

    @property
    def account_info(self):
        return self.services.session.account_info

    def copy_file(
        self,
        copy_source,
        file_name,
        content_type,
        file_info,
        destination_bucket_id,
        progress_listener,
        destination_encryption: EncryptionSetting | None = None,
        source_encryption: EncryptionSetting | None = None,
        legal_hold: LegalHold | None = None,
        file_retention: FileRetentionSetting | None = None,
    ):
        # Run small copies in the same thread pool as large file copies,
        # so that they share resources during a sync.
        return self._thread_pool.submit(
            self._copy_small_file,
            copy_source,
            file_name,
            content_type=content_type,
            file_info=file_info,
            destination_bucket_id=destination_bucket_id,
            progress_listener=progress_listener,
            destination_encryption=destination_encryption,
            source_encryption=source_encryption,
            legal_hold=legal_hold,
            file_retention=file_retention,
        )

    def copy_part(
        self,
        large_file_id,
        part_copy_source,
        part_number,
        large_file_upload_state,
        finished_parts=None,
        destination_encryption: EncryptionSetting | None = None,
        source_encryption: EncryptionSetting | None = None,
    ):
        return self._thread_pool.submit(
            self._copy_part,
            large_file_id,
            part_copy_source,
            part_number,
            large_file_upload_state,
            finished_parts=finished_parts,
            destination_encryption=destination_encryption,
            source_encryption=source_encryption,
        )

    def _copy_part(
        self,
        large_file_id,
        part_copy_source,
        part_number,
        large_file_upload_state,
        finished_parts,
        destination_encryption: EncryptionSetting | None,
        source_encryption: EncryptionSetting | None,
    ):
        """
        Copy a file part to started large file.

        :param :param str bucket_id: a bucket ID
        :param large_file_id: a large file ID
        :param b2sdk.v2.CopySource part_copy_source: copy source that represents a range (not necessarily a whole file)
        :param b2sdk.v2.LargeFileUploadState large_file_upload_state: state object for progress reporting
                                                                      on large file upload
        :param dict,None finished_parts: dictionary of known finished parts, keys are part numbers,
                                         values are instances of :class:`~b2sdk.v2.Part`
        :param b2sdk.v2.EncryptionSetting destination_encryption: encryption settings for the destination
                        (``None`` if unknown)
        :param b2sdk.v2.EncryptionSetting source_encryption: encryption settings for the source
                        (``None`` if unknown)
        """
        # b2_copy_part doesn't need SSE-B2. Large file encryption is decided on b2_start_large_file.
        if destination_encryption is not None and destination_encryption.mode == EncryptionMode.SSE_B2:
            destination_encryption = None

        # Check if this part was uploaded before
        if finished_parts is not None and part_number in finished_parts:
            # Report this part finished
            part = finished_parts[part_number]
            large_file_upload_state.update_part_bytes(part.content_length)

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # if another part has already had an error there's no point in
        # uploading this part
        if large_file_upload_state.has_error():
            raise AlreadyFailed(large_file_upload_state.get_error_message())

        response = self.services.session.copy_part(
            part_copy_source.file_id,
            large_file_id,
            part_number,
            bytes_range=part_copy_source.get_bytes_range(),
            destination_server_side_encryption=destination_encryption,
            source_server_side_encryption=source_encryption,
        )
        large_file_upload_state.update_part_bytes(response['contentLength'])
        return response

    def _copy_small_file(
        self,
        copy_source,
        file_name,
        content_type,
        file_info,
        destination_bucket_id,
        progress_listener: AbstractProgressListener,
        destination_encryption: EncryptionSetting | None,
        source_encryption: EncryptionSetting | None,
        legal_hold: LegalHold | None = None,
        file_retention: FileRetentionSetting | None = None,
    ):
        progress_listener.set_total_bytes(copy_source.get_content_length() or 0)

        bytes_range = copy_source.get_bytes_range()

        if content_type is None:
            if file_info is not None:
                raise CopyArgumentsMismatch('File info can be set only when content type is set')
            metadata_directive = MetadataDirectiveMode.COPY
        else:
            if file_info is None:
                raise CopyArgumentsMismatch(
                    'File info can be not set only when content type is not set'
                )
            metadata_directive = MetadataDirectiveMode.REPLACE
        metadata_directive, file_info, content_type = self.establish_sse_c_file_metadata(
            metadata_directive=metadata_directive,
            destination_file_info=file_info,
            destination_content_type=content_type,
            destination_server_side_encryption=destination_encryption,
            source_server_side_encryption=source_encryption,
            source_file_info=copy_source.source_file_info,
            source_content_type=copy_source.source_content_type,
        )
        response = self.services.session.copy_file(
            copy_source.file_id,
            file_name,
            bytes_range=bytes_range,
            metadata_directive=metadata_directive,
            content_type=content_type,
            file_info=file_info,
            destination_bucket_id=destination_bucket_id,
            destination_server_side_encryption=destination_encryption,
            source_server_side_encryption=source_encryption,
            legal_hold=legal_hold,
            file_retention=file_retention,
        )
        file_version = self.services.api.file_version_factory.from_api_response(response)
        progress_listener.bytes_completed(file_version.size)

        return file_version

    @classmethod
    def establish_sse_c_file_metadata(
        cls,
        metadata_directive: MetadataDirectiveMode,
        destination_file_info: dict | None,
        destination_content_type: str | None,
        destination_server_side_encryption: EncryptionSetting | None,
        source_server_side_encryption: EncryptionSetting | None,
        source_file_info: dict | None,
        source_content_type: str | None,
    ):
        assert metadata_directive in (MetadataDirectiveMode.REPLACE, MetadataDirectiveMode.COPY)

        if metadata_directive == MetadataDirectiveMode.REPLACE:
            if destination_server_side_encryption:
                destination_file_info = destination_server_side_encryption.add_key_id_to_file_info(
                    destination_file_info
                )
            return metadata_directive, destination_file_info, destination_content_type

        source_key_id = None
        destination_key_id = None

        if destination_server_side_encryption is not None and destination_server_side_encryption.key is not None and \
                destination_server_side_encryption.key.key_id is not None:
            destination_key_id = destination_server_side_encryption.key.key_id

        if source_server_side_encryption is not None and source_server_side_encryption.key is not None and \
                source_server_side_encryption.key.key_id is not None:
            source_key_id = source_server_side_encryption.key.key_id

        if source_key_id == destination_key_id:
            return metadata_directive, destination_file_info, destination_content_type

        if source_file_info is None or source_content_type is None:
            raise SSECKeyIdMismatchInCopy(
                'attempting to copy file using {} without providing source_file_info '
                'and source_content_type for differing sse_c_key_ids: source="{}", '
                'destination="{}"'.format(
                    MetadataDirectiveMode.COPY, source_key_id, destination_key_id
                )
            )

        destination_file_info = source_file_info.copy()
        destination_file_info.pop(SSE_C_KEY_ID_FILE_INFO_KEY_NAME, None)
        if destination_server_side_encryption:
            destination_file_info = destination_server_side_encryption.add_key_id_to_file_info(
                destination_file_info
            )
        destination_content_type = source_content_type

        return MetadataDirectiveMode.REPLACE, destination_file_info, destination_content_type
