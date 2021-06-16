######################################################################
#
# File: b2sdk/transfer/outbound/upload_manager.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import concurrent.futures as futures

from typing import Optional

from b2sdk.encryption.setting import EncryptionMode, EncryptionSetting
from b2sdk.exception import (
    AlreadyFailed,
    B2Error,
    MaxRetriesExceeded,
)
from b2sdk.file_lock import FileRetentionSetting, LegalHold
from b2sdk.stream.progress import ReadingStreamWithProgress
from b2sdk.stream.hashing import StreamWithHash
from b2sdk.http_constants import HEX_DIGITS_AT_END
from b2sdk.utils import B2TraceMetaAbstract

from .progress_reporter import PartProgressReporter

logger = logging.getLogger(__name__)


class UploadManager(metaclass=B2TraceMetaAbstract):
    """
    Handle complex actions around uploads to free raw_api from that responsibility.
    """

    MAX_UPLOAD_ATTEMPTS = 5

    def __init__(self, services, max_upload_workers=10):
        """
        :param b2sdk.v1.Services services:
        :param int max_upload_workers: a number of upload threads
        """
        self.services = services

        self.upload_executor = None
        self.max_workers = max_upload_workers

    @property
    def account_info(self):
        return self.services.session.account_info

    def set_thread_pool_size(self, max_workers):
        """
        Set the size of the thread pool to use for uploads and downloads.

        Must be called before any work starts, or the thread pool will get
        the default size.

        :param int max_workers: maximum allowed number of workers in a pool
        """
        if self.upload_executor is not None:
            raise Exception('thread pool already created')
        self.max_workers = max_workers

    def get_thread_pool(self):
        """
        Return the thread pool executor to use for uploads and downloads.
        """
        if self.upload_executor is None:
            self.upload_executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        return self.upload_executor

    def upload_file(
        self,
        bucket_id,
        upload_source,
        file_name,
        content_type,
        file_info,
        progress_listener,
        encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        f = self.get_thread_pool().submit(
            self._upload_small_file,
            bucket_id,
            upload_source,
            file_name,
            content_type,
            file_info,
            progress_listener,
            encryption,
            file_retention,
            legal_hold,
        )
        return f

    def upload_part(
        self,
        bucket_id,
        file_id,
        part_upload_source,
        part_number,
        large_file_upload_state,
        finished_parts=None,
        encryption: EncryptionSetting = None,
    ):
        f = self.get_thread_pool().submit(
            self._upload_part,
            bucket_id,
            file_id,
            part_upload_source,
            part_number,
            large_file_upload_state,
            finished_parts,
            encryption,
        )
        return f

    def _upload_part(
        self,
        bucket_id,
        file_id,
        part_upload_source,
        part_number,
        large_file_upload_state,
        finished_parts,
        encryption: EncryptionSetting,
    ):
        """
        Upload a file part to started large file.

        :param :param str bucket_id: a bucket ID
        :param file_id: a large file ID
        :param b2sdk.v1.UploadSourcePart upload_source_part: wrapper for upload source that reads only required range
        :param b2sdk.v1.LargeFileUploadState large_file_upload_state: state object for progress reporting
                                                                      on large file upload
        :param dict,None finished_parts: dictionary of known finished parts, keys are part numbers,
                                         values are instances of :class:`~b2sdk.v1.Part`
        :param b2sdk.v1.EncryptionSetting encryption: encryption setting (``None`` if unknown)
        """

        # b2_upload_part doesn't need SSE-B2. Large file encryption is decided on b2_start_large_file.
        if encryption is not None and encryption.mode == EncryptionMode.SSE_B2:
            encryption = None

        # Check if this part was uploaded before
        if finished_parts is not None and part_number in finished_parts:
            # Report this part finished
            part = finished_parts[part_number]
            large_file_upload_state.update_part_bytes(part_upload_source.get_content_length())

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # Set up a progress listener
        part_progress_listener = PartProgressReporter(large_file_upload_state)

        # Retry the upload as needed
        exception_list = []
        for _ in range(self.MAX_UPLOAD_ATTEMPTS):
            # if another part has already had an error there's no point in
            # uploading this part
            if large_file_upload_state.has_error():
                raise AlreadyFailed(large_file_upload_state.get_error_message())

            try:
                with part_upload_source.open() as part_stream:
                    content_length = part_upload_source.get_content_length()
                    input_stream = ReadingStreamWithProgress(
                        part_stream, part_progress_listener, length=content_length
                    )
                    if part_upload_source.is_sha1_known():
                        content_sha1 = part_upload_source.get_content_sha1()
                    else:
                        input_stream = StreamWithHash(input_stream, stream_length=content_length)
                        content_sha1 = HEX_DIGITS_AT_END
                    # it is important that `len()` works on `input_stream`
                    response = self.services.session.upload_part(
                        file_id,
                        part_number,
                        len(input_stream),
                        content_sha1,
                        input_stream,
                        server_side_encryption=encryption,  # todo: client side encryption
                    )
                    if content_sha1 == HEX_DIGITS_AT_END:
                        content_sha1 = input_stream.hash
                    assert content_sha1 == response['contentSha1']
                    return response

            except B2Error as e:
                if not e.should_retry_upload():
                    raise
                exception_list.append(e)
                self.account_info.clear_bucket_upload_data(bucket_id)

        large_file_upload_state.set_error(str(exception_list[-1]))
        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_list)

    def _upload_small_file(
        self,
        bucket_id,
        upload_source,
        file_name,
        content_type,
        file_info,
        progress_listener,
        encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        content_length = upload_source.get_content_length()
        exception_info_list = []
        progress_listener.set_total_bytes(content_length)
        with progress_listener:
            for _ in range(self.MAX_UPLOAD_ATTEMPTS):
                try:
                    with upload_source.open() as file:
                        input_stream = ReadingStreamWithProgress(
                            file, progress_listener, length=content_length
                        )
                        if upload_source.is_sha1_known():
                            content_sha1 = upload_source.get_content_sha1()
                        else:
                            input_stream = StreamWithHash(
                                input_stream, stream_length=content_length
                            )
                            content_sha1 = HEX_DIGITS_AT_END
                        # it is important that `len()` works on `input_stream`
                        response = self.services.session.upload_file(
                            bucket_id,
                            file_name,
                            len(input_stream),
                            content_type,
                            content_sha1,
                            file_info,
                            input_stream,
                            server_side_encryption=encryption,  # todo: client side encryption
                            file_retention=file_retention,
                            legal_hold=legal_hold,
                        )
                        if content_sha1 == HEX_DIGITS_AT_END:
                            content_sha1 = input_stream.hash
                        assert content_sha1 == response[
                            'contentSha1'], '%s != %s' % (content_sha1, response['contentSha1'])
                        return self.services.api.file_version_factory.from_api_response(response)

                except B2Error as e:
                    if not e.should_retry_upload():
                        raise
                    exception_info_list.append(e)
                    self.account_info.clear_bucket_upload_data(bucket_id)

        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_info_list)
