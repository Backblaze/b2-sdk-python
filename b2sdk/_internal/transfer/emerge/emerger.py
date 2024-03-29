######################################################################
#
# File: b2sdk/_internal/transfer/emerge/emerger.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
from typing import Iterator

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.file_lock import FileRetentionSetting, LegalHold
from b2sdk._internal.http_constants import LARGE_FILE_SHA1
from b2sdk._internal.progress import AbstractProgressListener
from b2sdk._internal.transfer.emerge.executor import EmergeExecutor
from b2sdk._internal.transfer.emerge.planner.planner import EmergePlan, EmergePlanner
from b2sdk._internal.transfer.emerge.write_intent import WriteIntent
from b2sdk._internal.utils import B2TraceMetaAbstract, Sha1HexDigest, iterator_peek

logger = logging.getLogger(__name__)


class Emerger(metaclass=B2TraceMetaAbstract):
    """
    Handle complex actions around multi source copy/uploads.

    This class can be used to build advanced copy workflows like incremental upload.

    It creates a emerge plan and pass it to emerge executor - all complex logic
    is actually implemented in :class:`b2sdk._internal.transfer.emerge.planner.planner.EmergePlanner`
    and :class:`b2sdk._internal.transfer.emerge.executor.EmergeExecutor`
    """

    DEFAULT_STREAMING_MAX_QUEUE_SIZE = 100

    def __init__(self, services):
        """
        :param b2sdk.v2.Services services:
        """
        self.services = services
        self.emerge_executor = EmergeExecutor(services)

    @classmethod
    def _get_updated_file_info_with_large_file_sha1(
        cls,
        file_info: dict[str, str] | None,
        write_intents: list[WriteIntent] | None,
        emerge_plan: EmergePlan,
        large_file_sha1: Sha1HexDigest | None = None,
    ) -> dict[str, str] | None:
        if not emerge_plan.is_large_file():
            # Emerge plan doesn't construct a large file, no point setting the large_file_sha1
            return file_info

        file_sha1 = large_file_sha1
        if not file_sha1 and write_intents is not None and len(write_intents) == 1:
            # large_file_sha1 was not given explicitly, but there's just one write intent, perhaps it has a hash
            file_sha1 = write_intents[0].get_content_sha1()

        out_file_info = file_info
        if file_sha1:
            out_file_info = dict(file_info) if file_info else {}
            out_file_info[LARGE_FILE_SHA1] = file_sha1

        return out_file_info

    def _emerge(
        self,
        emerge_function,
        bucket_id,
        write_intents_iterable,
        file_name,
        content_type,
        file_info,
        progress_listener,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        max_queue_size=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        check_first_intent_for_sha1: bool = True,
        custom_upload_timestamp: int | None = None,
    ):
        planner = self.get_emerge_planner(
            min_part_size=min_part_size,
            recommended_upload_part_size=recommended_upload_part_size,
            max_part_size=max_part_size,
        )

        # Large file SHA1 operation, possibly on intents.
        large_file_sha1_intents_for_check = None
        all_write_intents = write_intents_iterable
        if check_first_intent_for_sha1:
            write_intents_iterator = iter(all_write_intents)
            large_file_sha1_intents_for_check, all_write_intents = \
                iterator_peek(write_intents_iterator, 2)

        emerge_plan = emerge_function(planner, all_write_intents)

        out_file_info = self._get_updated_file_info_with_large_file_sha1(
            file_info,
            large_file_sha1_intents_for_check,
            emerge_plan,
            large_file_sha1,
        )

        return self.emerge_executor.execute_emerge_plan(
            emerge_plan,
            bucket_id,
            file_name,
            content_type,
            out_file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            # Max queue size is only used in case of large files.
            # Passing anything for small files does nothing.
            max_queue_size=max_queue_size,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def emerge(
        self,
        bucket_id: str,
        write_intents: list[WriteIntent],
        file_name: str,
        content_type: str | None,
        file_info: dict[str, str] | None,
        progress_listener: AbstractProgressListener,
        recommended_upload_part_size: int | None = None,
        continue_large_file_id: str | None = None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        """
        Create a new file (object in the cloud, really) from an iterable (list, tuple etc) of write intents.

        :param bucket_id: a bucket ID
        :param write_intents: write intents to process to create a file
        :param file_name: the file name of the new B2 file
        :param content_type: the MIME type or ``None`` to determine automatically
        :param file_info: a file info to store with the file or ``None`` to not store anything
        :param progress_listener: a progress listener object to use
        :param recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, if ``None`` in multipart case it would always start a new
                        large file
        :param encryption: encryption settings (``None`` if unknown)
        :param file_retention: file retention setting
        :param legal_hold: legal hold setting
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param large_file_sha1: SHA1 for this file, if ``None`` and there's exactly one intent, it'll be taken from it
        :param custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        """
        return self._emerge(
            EmergePlanner.get_emerge_plan,
            bucket_id,
            write_intents,
            file_name,
            content_type,
            file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            recommended_upload_part_size=recommended_upload_part_size,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def emerge_stream(
        self,
        bucket_id: str,
        write_intent_iterator: Iterator[WriteIntent],
        file_name: str,
        content_type: str | None,
        file_info: dict[str, str] | None,
        progress_listener: AbstractProgressListener,
        recommended_upload_part_size: int | None = None,
        continue_large_file_id: str | None = None,
        max_queue_size: int = DEFAULT_STREAMING_MAX_QUEUE_SIZE,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        """
        Create a new file (object in the cloud, really) from a stream of write intents.

        :param bucket_id: a bucket ID
        :param write_intent_iterator: iterator of :class:`~b2sdk.v2.WriteIntent`
        :param file_name: the file name of the new B2 file
        :param content_type: the MIME type or ``None`` to determine automatically
        :param file_info: a file info to store with the file or ``None`` to not store anything
        :param progress_listener: a progress listener object to use
        :param recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, if ``None`` in multipart case it would always start a new
                        large file
        :param max_queue_size: parallelization level
        :param encryption: encryption settings (``None`` if unknown)
        :param file_retention: file retention setting
        :param legal_hold: legal hold setting
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param large_file_sha1: SHA1 for this file, if ``None`` and there's exactly one intent, it'll be taken from it
        :param custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        """
        return self._emerge(
            EmergePlanner.get_streaming_emerge_plan,
            bucket_id,
            write_intent_iterator,
            file_name,
            content_type,
            file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            max_queue_size=max_queue_size,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            recommended_upload_part_size=recommended_upload_part_size,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def emerge_unbound(
        self,
        bucket_id: str,
        write_intent_iterator: Iterator[WriteIntent],
        file_name: str,
        content_type: str | None,
        file_info: dict[str, str] | None,
        progress_listener: AbstractProgressListener,
        recommended_upload_part_size: int | None = None,
        continue_large_file_id: str | None = None,
        max_queue_size: int = 1,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        """
        Create a new file (object in the cloud, really) from an unbound stream of write intents.

        :param bucket_id: a bucket ID
        :param write_intent_iterator: iterator of :class:`~b2sdk.v2.WriteIntent`
        :param file_name: the file name of the new B2 file
        :param content_type: the MIME type or ``None`` to determine automatically
        :param file_info: a file info to store with the file or ``None`` to not store anything
        :param progress_listener: a progress listener object to use
        :param recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, if ``None`` in multipart case it would always start a new
                        large file
        :param max_queue_size: parallelization level, should be equal to the number of buffers available in parallel
        :param encryption: encryption settings (``None`` if unknown)
        :param file_retention: file retention setting
        :param legal_hold: legal hold setting
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param large_file_sha1: SHA1 for this file, if ``None`` it's left unset
        :param custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        """
        return self._emerge(
            EmergePlanner.get_unbound_emerge_plan,
            bucket_id,
            write_intent_iterator,
            file_name,
            content_type,
            file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            max_queue_size=max_queue_size,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            recommended_upload_part_size=recommended_upload_part_size,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            check_first_intent_for_sha1=False,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def get_emerge_planner(
        self,
        recommended_upload_part_size: int | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
    ):
        return EmergePlanner.from_account_info(
            self.services.session.account_info,
            min_part_size=min_part_size,
            recommended_upload_part_size=recommended_upload_part_size,
            max_part_size=max_part_size,
        )
