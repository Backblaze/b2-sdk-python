######################################################################
#
# File: test/integration/bucket_cleaner.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional

from b2sdk.v2 import *

from .helpers import GENERAL_BUCKET_NAME_PREFIX, BUCKET_CREATED_AT_MILLIS, authorize

ONE_HOUR_MILLIS = 60 * 60 * 1000


class BucketCleaner:
    def __init__(
        self,
        dont_cleanup_old_buckets: bool,
        b2_application_key_id: str,
        b2_application_key: str,
        current_run_prefix: Optional[str] = None
    ):
        self.current_run_prefix = current_run_prefix
        self.dont_cleanup_old_buckets = dont_cleanup_old_buckets
        self.b2_application_key_id = b2_application_key_id
        self.b2_application_key = b2_application_key

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
        b2_api, _ = authorize((self.b2_application_key_id, self.b2_application_key))
        buckets = b2_api.list_buckets()
        for bucket in buckets:
            if not self._should_remove_bucket(bucket):
                print('Skipping bucket removal:', bucket.name)
            else:
                print('Trying to remove bucket:', bucket.name)
                files_leftover = False
                file_versions = bucket.ls(latest_only=False, recursive=True)
                for file_version_info, _ in file_versions:
                    if file_version_info.file_retention:
                        if file_version_info.file_retention.mode == RetentionMode.GOVERNANCE:
                            print('Removing retention from file version:', file_version_info.id_)
                            b2_api.update_file_retention(
                                file_version_info.id_, file_version_info.file_name,
                                NO_RETENTION_FILE_SETTING, True
                            )
                        elif file_version_info.file_retention.mode == RetentionMode.COMPLIANCE:
                            if file_version_info.file_retention.retain_until > current_time_millis():  # yapf: disable
                                print(
                                    'File version: %s cannot be removed due to compliance mode retention'
                                    % (file_version_info.id_,)
                                )
                                files_leftover = True
                                continue
                        elif file_version_info.file_retention.mode == RetentionMode.NONE:
                            pass
                        else:
                            raise ValueError(
                                'Unknown retention mode: %s' %
                                (file_version_info.file_retention.mode,)
                            )
                    if file_version_info.legal_hold.is_on():
                        print('Removing legal hold from file version:', file_version_info.id_)
                        b2_api.update_file_legal_hold(
                            file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                        )
                    print('Removing file version:', file_version_info.id_)
                    b2_api.delete_file_version(file_version_info.id_, file_version_info.file_name)

                if files_leftover:
                    print('Unable to remove bucket because some retained files remain')
                else:
                    print('Removing bucket:', bucket.name)
                    b2_api.delete_bucket(bucket)
