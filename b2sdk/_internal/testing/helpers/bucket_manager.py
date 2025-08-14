######################################################################
#
# File: b2sdk/_internal/testing/helpers/bucket_manager.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import platform
from collections.abc import Iterable
from datetime import datetime, timedelta
from itertools import chain
from typing import Any

import tenacity

from b2sdk._internal.api import B2Api
from b2sdk._internal.bucket import Bucket
from b2sdk._internal.exception import BadRequest, BucketIdNotFound, FileNotPresent, TooManyRequests
from b2sdk._internal.file_lock import NO_RETENTION_FILE_SETTING, LegalHold, RetentionMode
from b2sdk._internal.testing.helpers.buckets import (
    BUCKET_CREATED_AT_MILLIS,
    BUCKET_NAME_LENGTH,
    GENERAL_BUCKET_NAME_PREFIX,
    random_token,
)
from b2sdk._internal.utils import current_time_millis

NODE_DESCRIPTION = f'{platform.node()}: {platform.platform()}'
ONE_HOUR_MILLIS = 60 * 60 * 1000
BUCKET_CLEANUP_PERIOD_MILLIS = timedelta(hours=3).total_seconds() * 1000

logger = logging.getLogger(__name__)


class BucketManager:
    def __init__(
        self,
        dont_cleanup_old_buckets: bool,
        b2_api: B2Api,
        current_run_prefix: str = '',
        general_prefix: str = GENERAL_BUCKET_NAME_PREFIX,
    ):
        self.current_run_prefix = current_run_prefix
        self.general_prefix = general_prefix
        self.dont_cleanup_old_buckets = dont_cleanup_old_buckets
        self.b2_api = b2_api
        self.bucket_name_log: list[str] = []

    def new_bucket_name(self) -> str:
        bucket_name = self.current_run_prefix + random_token(
            BUCKET_NAME_LENGTH - len(self.current_run_prefix)
        )
        self.bucket_name_log.append(bucket_name)
        return bucket_name

    def new_bucket_info(self) -> dict:
        return {
            BUCKET_CREATED_AT_MILLIS: str(current_time_millis()),
            'created_by': NODE_DESCRIPTION,
        }

    def create_bucket(self, bucket_type: str = 'allPublic', **kwargs) -> Bucket:
        bucket_name = kwargs.pop('name', self.new_bucket_name())
        return self.b2_api.create_bucket(
            bucket_name,
            bucket_type=bucket_type,
            bucket_info=self.new_bucket_info(),
            **kwargs,
        )

    def _should_remove_bucket(self, bucket: Bucket) -> tuple[bool, str]:
        if self.current_run_prefix and bucket.name.startswith(self.current_run_prefix):
            return True, 'it is a bucket for this very run'
        if self.dont_cleanup_old_buckets:
            return False, 'old buckets ought not to be cleaned'
        if bucket.name.startswith(self.general_prefix):
            if BUCKET_CREATED_AT_MILLIS in bucket.bucket_info:
                delete_older_than = current_time_millis() - BUCKET_CLEANUP_PERIOD_MILLIS
                this_bucket_creation_time = int(bucket.bucket_info[BUCKET_CREATED_AT_MILLIS])
                if this_bucket_creation_time < delete_older_than:
                    return (
                        True,
                        f'this_bucket_creation_time={this_bucket_creation_time} < delete_older_than={delete_older_than}',
                    )
                return (
                    False,
                    f'this_bucket_creation_time={this_bucket_creation_time} >= delete_older_than={delete_older_than}',
                )
            else:
                return True, 'undefined ' + BUCKET_CREATED_AT_MILLIS
        return False, f'does not start with {self.general_prefix!r}'

    def clean_buckets(self, quick=False):
        # even with use_cache=True, if cache is empty API call will be made
        buckets = self.b2_api.list_buckets(use_cache=quick)
        remaining_buckets = []
        for bucket in buckets:
            should_remove, why = self._should_remove_bucket(bucket)
            if not should_remove:
                print(f'Skipping bucket removal {bucket.name!r} because {why}')
                remaining_buckets.append(bucket)
                continue

            print('Trying to remove bucket:', bucket.name, 'because', why)
            try:
                self.clean_bucket(bucket)
            except BucketIdNotFound:
                print(f'It seems that bucket {bucket.name} has already been removed')
        print('Total bucket count after cleanup:', len(remaining_buckets))
        for bucket in remaining_buckets:
            print(bucket)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(TooManyRequests),
        wait=tenacity.wait_exponential(),
        stop=tenacity.stop_after_attempt(8),
    )
    def clean_bucket(
        self,
        bucket: Bucket | str,
        only_files: bool = False,
        only_folders: list[str] | None = None,
        ignore_retentions: bool = False,
    ):
        """
        Clean contents of bucket, by default also deleting the bucket.

        Args:
            bucket (Bucket | str): Bucket object or name
            only_files (bool): If to only delete files and not the bucket
            only_folders (list[str] | None): If not None, filter to only files in given folders.
            ignore_retentions (bool): If deletion should happen regardless of files' retention mode.
        """
        if isinstance(bucket, str):
            bucket = self.b2_api.get_bucket_by_name(bucket)

        if not only_files:
            # try optimistic bucket removal first, since it is completely free (as opposed to `ls` call)
            try:
                return self.b2_api.delete_bucket(bucket)
            except BucketIdNotFound:
                return  # bucket was already removed
            except BadRequest as exc:
                assert exc.code == 'cannot_delete_non_empty_bucket'

        files_leftover = False

        file_versions: Iterable[Any]
        if only_folders:
            file_versions = chain.from_iterable(
                [
                    bucket.ls(
                        path=folder,
                        latest_only=False,
                        recursive=True,
                    )
                    for folder in only_folders
                ]
            )
        else:
            file_versions = bucket.ls(latest_only=False, recursive=True)

        for file_version_info, _ in file_versions:
            if file_version_info.file_retention and not ignore_retentions:
                if file_version_info.file_retention.mode == RetentionMode.GOVERNANCE:
                    print('Removing retention from file version:', file_version_info.id_)
                    self.b2_api.update_file_retention(
                        file_version_info.id_,
                        file_version_info.file_name,
                        NO_RETENTION_FILE_SETTING,
                        True,
                    )
                elif file_version_info.file_retention.mode == RetentionMode.COMPLIANCE:
                    if file_version_info.file_retention.retain_until > current_time_millis():
                        print(
                            f'File version: {file_version_info.id_} cannot be removed due to compliance mode retention'
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
                print('Removing legal hold from file version:', file_version_info.id_)
                self.b2_api.update_file_legal_hold(
                    file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                )
            print('Removing file version:', file_version_info.id_)
            try:
                self.b2_api.delete_file_version(file_version_info.id_, file_version_info.file_name)
            except FileNotPresent:
                print(
                    f'It seems that file version {file_version_info.id_} has already been removed'
                )

        if files_leftover:
            print('Unable to remove bucket because some retained files remain')
        elif not only_files:
            print('Removing bucket:', bucket.name)
            try:
                self.b2_api.delete_bucket(bucket)
            except BucketIdNotFound:
                print(f'It seems that bucket {bucket.name} has already been removed')
        print()

    def count_and_print_buckets(self) -> int:
        buckets = self.b2_api.list_buckets()
        count = len(buckets)
        print(f'Total bucket count at {datetime.now()}: {count}')
        for i, bucket in enumerate(buckets, start=1):
            print(f'- {i}\t{bucket.name} [{bucket.id_}]')
        return count
