######################################################################
#
# File: b2sdk/transfer/emerge/emerger.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
from typing import Dict, Optional, List

from b2sdk.encryption.setting import EncryptionSetting
from b2sdk.file_lock import FileRetentionSetting, LegalHold
from b2sdk.http_constants import LARGE_FILE_SHA1
from b2sdk.transfer.emerge.executor import EmergeExecutor
from b2sdk.transfer.emerge.planner.planner import EmergePlan, EmergePlanner
from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.utils import B2TraceMetaAbstract, iterator_peek, Sha1HexDigest

logger = logging.getLogger(__name__)


class Emerger(metaclass=B2TraceMetaAbstract):
    """
    Handle complex actions around multi source copy/uploads.

    This class can be used to build advanced copy workflows like incremental upload.

    It creates a emerge plan and pass it to emerge executor - all complex logic
    is actually implemented in :class:`b2sdk.transfer.emerge.planner.planner.EmergePlanner`
    and :class:`b2sdk.transfer.emerge.executor.EmergeExecutor`
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
        file_info: Optional[Dict[str, str]],
        write_intents: List[WriteIntent],
        emerge_plan: EmergePlan,
        large_file_sha1: Optional[Sha1HexDigest] = None,
    ) -> Optional[Dict[str, str]]:
        if not emerge_plan.is_large_file():
            # Emerge plan doesn't construct a large file, no point setting the large_file_sha1
            return file_info

        file_sha1 = large_file_sha1
        if not file_sha1 and len(write_intents) == 1:
            # large_file_sha1 was not given explicitly, but there's just one write intent, perhaps it has a hash
            file_sha1 = write_intents[0].get_content_sha1()

        out_file_info = file_info
        if file_sha1:
            out_file_info = dict(file_info) if file_info else {}
            out_file_info[LARGE_FILE_SHA1] = file_sha1

        return out_file_info

    def emerge(
        self,
        bucket_id,
        write_intents,
        file_name,
        content_type,
        file_info,
        progress_listener,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
        min_part_size=None,
        max_part_size=None,
        large_file_sha1=None,
    ):
        """
        Create a new file (object in the cloud, really) from an iterable (list, tuple etc) of write intents.

        :param str bucket_id: a bucket ID
        :param write_intents: write intents to process to create a file
        :type write_intents: List[b2sdk.v2.WriteIntent]
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type or ``None`` to determine automatically
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v2.AbstractProgressListener progress_listener: a progress listener object to use

        :param int min_part_size: lower limit of part size for the transfer planner, in bytes
        :param int max_part_size: upper limit of part size for the transfer planner, in bytes
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        """
        # WARNING: time spent trying to extract common parts of emerge() and emerge_stream()
        # into a separate method: 20min. You can try it too, but please increment the timer honestly.
        # Problematic lines are marked with a "<--".
        planner = self.get_emerge_planner(
            min_part_size=min_part_size,
            recommended_upload_part_size=recommended_upload_part_size,
            max_part_size=max_part_size,
        )
        emerge_plan = planner.get_emerge_plan(write_intents)  # <--

        updated_file_info = self._get_updated_file_info_with_large_file_sha1(
            file_info, write_intents, emerge_plan, large_file_sha1
        )

        return self.emerge_executor.execute_emerge_plan(
            emerge_plan,
            bucket_id,
            file_name,
            content_type,
            updated_file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def emerge_stream(
        self,
        bucket_id,
        write_intent_iterator,
        file_name,
        content_type,
        file_info,
        progress_listener,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        max_queue_size=DEFAULT_STREAMING_MAX_QUEUE_SIZE,
        encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
        min_part_size=None,
        max_part_size=None,
        large_file_sha1=None,
    ):
        """
        Create a new file (object in the cloud, really) from a stream of write intents.

        :param str bucket_id: a bucket ID
        :param write_intent_iterator: iterator of :class:`~b2sdk.v2.WriteIntent`
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type or ``None`` to determine automatically
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v2.AbstractProgressListener progress_listener: a progress listener object to use
        :param int,None recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param str,None continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, if ``None`` in multipart case it would always start a new
                        large file
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting

        :param int min_part_size: lower limit of part size for the transfer planner, in bytes
        :param int max_part_size: upper limit of part size for the transfer planner, in bytes
        :param Sha1HexDigest,None large_file_sha1: result file SHA1 hash or ``None`` if not known
        """
        planner = self.get_emerge_planner(
            min_part_size=min_part_size,
            recommended_upload_part_size=recommended_upload_part_size,
            max_part_size=max_part_size,
        )

        # iter(iterable) –> iterator; iter(iterator) –> iterator.
        intent_iterator = iter(write_intent_iterator)

        first_write_intents, intent_iterator = iterator_peek(intent_iterator, 2)

        emerge_plan = planner.get_streaming_emerge_plan(intent_iterator)  # <--

        updated_file_info = self._get_updated_file_info_with_large_file_sha1(
            file_info, first_write_intents, emerge_plan, large_file_sha1
        )

        return self.emerge_executor.execute_emerge_plan(
            emerge_plan,
            bucket_id,
            file_name,
            content_type,
            updated_file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            max_queue_size=max_queue_size,  # <--
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def get_emerge_planner(
        self,
        recommended_upload_part_size=None,
        min_part_size=None,
        max_part_size=None,
    ):
        return EmergePlanner.from_account_info(
            self.services.session.account_info,
            min_part_size=min_part_size,
            recommended_upload_part_size=recommended_upload_part_size,
            max_part_size=max_part_size,
        )
