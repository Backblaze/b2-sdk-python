######################################################################
#
# File: test/integration/bucket_cleaner.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging

from b2sdk.v2 import (
    NO_RETENTION_FILE_SETTING,
    B2Api,
    Bucket,
    LegalHold,
    RetentionMode,
    current_time_millis,
)
from b2sdk.v2.exception import BadRequest

from .helpers import BUCKET_CREATED_AT_MILLIS, GENERAL_BUCKET_NAME_PREFIX

ONE_HOUR_MILLIS = 60 * 60 * 1000

logger = logging.getLogger(__name__)


class BucketCleaner:
    def __init__(
        self, dont_cleanup_old_buckets: bool, b2_api: B2Api, current_run_prefix: str | None = None
    ):
        self.current_run_prefix = current_run_prefix
        self.dont_cleanup_old_buckets = dont_cleanup_old_buckets
        self.b2_api = b2_api

    def _should_remove_bucket(self, bucket: Bucket):
        if self.current_run_prefix and bucket.name.startswith(self.current_run_prefix):
            return True
        if self.dont_cleanup_old_buckets:
            return False
        if bucket.name.startswith(GENERAL_BUCKET_NAME_PREFIX):
            if BUCKET_CREATED_AT_MILLIS in bucket.bucket_info:
                if int(bucket.bucket_info[BUCKET_CREATED_AT_MILLIS]
                      ) < current_time_millis() - ONE_HOUR_MILLIS:
                    return True
        return False

    def cleanup_buckets(self):
        buckets = self.b2_api.list_buckets()
        for bucket in buckets:
            self.cleanup_bucket(bucket)

    def cleanup_bucket(self, bucket: Bucket):
        b2_api = self.b2_api
        if not self._should_remove_bucket(bucket):
            logger.info('Skipping bucket removal:', bucket.name)
        else:
            logger.info('Trying to remove bucket:', bucket.name)
            files_leftover = False
            try:
                b2_api.delete_bucket(bucket)
            except BadRequest:
                logger.info('Bucket is not empty, removing files')
                files_leftover = True

            if files_leftover:
                files_leftover = False
                file_versions = bucket.ls(latest_only=False, recursive=True)
                for file_version_info, _ in file_versions:
                    if file_version_info.file_retention:
                        if file_version_info.file_retention.mode == RetentionMode.GOVERNANCE:
                            logger.info(
                                'Removing retention from file version: %s', file_version_info.id_
                            )
                            b2_api.update_file_retention(
                                file_version_info.id_, file_version_info.file_name,
                                NO_RETENTION_FILE_SETTING, True
                            )
                        elif file_version_info.file_retention.mode == RetentionMode.COMPLIANCE:
                            if file_version_info.file_retention.retain_until > current_time_millis():  # yapf: disable
                                logger.info(
                                    'File version: %s cannot be removed due to compliance mode retention',
                                    file_version_info.id_,
                                )
                                files_leftover = True
                                continue
                        elif file_version_info.file_retention.mode == RetentionMode.NONE:
                            pass
                        else:
                            raise ValueError(
                                f'Unknown retention mode: {file_version_info.file_retention.mode}'
                            )
                    if file_version_info.legal_hold.is_on():
                        logger.info(
                            'Removing legal hold from file version: %s', file_version_info.id_
                        )
                        b2_api.update_file_legal_hold(
                            file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                        )
                    logger.info('Removing file version:', file_version_info.id_)
                    b2_api.delete_file_version(file_version_info.id_, file_version_info.file_name)

                if files_leftover:
                    logger.info('Unable to remove bucket because some retained files remain')
                    return
                else:
                    b2_api.delete_bucket(bucket)
            logger.info('Removed bucket:', bucket.name)
