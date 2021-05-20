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
from typing import Optional

from b2sdk.encryption.setting import EncryptionSetting
from b2sdk.file_lock import FileRetentionSetting, LegalHold
from b2sdk.utils import B2TraceMetaAbstract
from b2sdk.transfer.emerge.executor import EmergeExecutor
from b2sdk.transfer.emerge.planner.planner import EmergePlanner

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
        :param b2sdk.v1.Services services:
        """
        self.services = services
        self.emerge_executor = EmergeExecutor(services)

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
    ):
        """
        Create a new file (object in the cloud, really) from an iterable (list, tuple etc) of write intents.

        :param str bucket_id: a bucket ID
        :param write_intents: write intents to process to create a file
        :type write_intents: List[b2sdk.v1.WriteIntent]
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type or ``None`` to determine automatically
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v1.AbstractProgressListener progress_listener: a progress listener object to use

        """
        planner = self.get_emerge_planner(recommended_upload_part_size=recommended_upload_part_size)
        emerge_plan = planner.get_emerge_plan(write_intents)
        return self.emerge_executor.execute_emerge_plan(
            emerge_plan,
            bucket_id,
            file_name,
            content_type,
            file_info,
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
    ):
        """
        Create a new file (object in the cloud, really) from a stream of write intents.

        :param str bucket_id: a bucket ID
        :param write_intents: iterator of :class:`~b2sdk.v1.WriteIntent`
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type or ``None`` to determine automatically
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v1.AbstractProgressListener progress_listener: a progress listener object to use

        """
        planner = self.get_emerge_planner(recommended_upload_part_size=recommended_upload_part_size)
        emerge_plan = planner.get_streaming_emerge_plan(write_intent_iterator)
        return self.emerge_executor.execute_emerge_plan(
            emerge_plan,
            bucket_id,
            file_name,
            content_type,
            file_info,
            progress_listener,
            continue_large_file_id=continue_large_file_id,
            max_queue_size=max_queue_size,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def get_emerge_planner(self, recommended_upload_part_size=None):
        return EmergePlanner.from_account_info(
            self.services.session.account_info,
            recommended_upload_part_size=recommended_upload_part_size,
        )
