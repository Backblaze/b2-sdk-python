######################################################################
#
# File: b2sdk/v2/api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
from typing import Generator

from b2sdk import v3
from b2sdk.v3.exception import BucketIdNotFound as v3BucketIdNotFound
from .bucket import Bucket, BucketFactory
from .exception import (
    BucketIdNotFound,
    RestrictedBucket,
    RestrictedBucketMissing,
    MissingAccountData,
)
from .raw_api import API_VERSION as RAW_API_VERSION
from .session import B2Session
from .transfer import DownloadManager, UploadManager
from .file_version import FileVersionFactory
from .large_file import LargeFileServices
from .application_key import FullApplicationKey, ApplicationKey, BaseApplicationKey
from .account_info import AbstractAccountInfo
from .api_config import DEFAULT_HTTP_API_CONFIG, B2HttpApiConfig


class Services(v3.Services):
    UPLOAD_MANAGER_CLASS = staticmethod(UploadManager)
    DOWNLOAD_MANAGER_CLASS = staticmethod(DownloadManager)
    LARGE_FILE_SERVICES_CLASS = staticmethod(LargeFileServices)


# override to use legacy B2Session with legacy B2Http
# and to raise old style BucketIdNotFound exception
# and to use old style Bucket
# and to use legacy authorize_account signature
class B2Api(v3.B2Api):
    SESSION_CLASS = staticmethod(B2Session)
    BUCKET_CLASS = staticmethod(Bucket)
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    SERVICES_CLASS = staticmethod(Services)
    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionFactory)
    APPLICATION_KEY_CLASS = ApplicationKey  # type: ignore
    FULL_APPLICATION_KEY_CLASS = FullApplicationKey  # type: ignore
    API_VERSION = RAW_API_VERSION

    # Legacy init in case something depends on max_workers defaults = 10
    def __init__(
        self,
        account_info: AbstractAccountInfo | None = None,
        cache: v3.AbstractCache | None = None,
        max_upload_workers: int | None = 10,
        max_copy_workers: int | None = 10,
        api_config: B2HttpApiConfig = DEFAULT_HTTP_API_CONFIG,
        max_download_workers: int | None = None,
        save_to_buffer_size: int | None = None,
        check_download_hash: bool = True,
        max_download_streams_per_file: int | None = None,
    ):
        super().__init__(
            account_info=account_info,
            cache=cache,
            max_upload_workers=max_upload_workers,
            max_copy_workers=max_copy_workers,
            api_config=api_config,
            max_download_workers=max_download_workers,
            save_to_buffer_size=save_to_buffer_size,
            check_download_hash=check_download_hash,
            max_download_streams_per_file=max_download_streams_per_file,
        )

    def get_bucket_by_id(self, bucket_id: str) -> v3.Bucket:
        try:
            return super().get_bucket_by_id(bucket_id)
        except v3BucketIdNotFound as e:
            raise BucketIdNotFound(e.bucket_id)

    # one could contemplate putting "@limit_trace_arguments(only=('self', 'realm'))" here but logfury meta magic copies
    # the appropriate attributes from base classes
    def authorize_account(self, realm, application_key_id, application_key):
        return super().authorize_account(
            application_key_id=application_key_id,
            application_key=application_key,
            realm=realm,
        )

    def create_key(  # type: ignore
        self,
        capabilities: list[str],
        key_name: str,
        valid_duration_seconds: int | None = None,
        bucket_id: str | None = None,
        name_prefix: str | None = None,
    ) -> FullApplicationKey:
        account_id = self.account_info.get_account_id()

        response = self.session.create_key(
            account_id,
            capabilities=capabilities,
            key_name=key_name,
            valid_duration_seconds=valid_duration_seconds,
            bucket_id=bucket_id,
            name_prefix=name_prefix,
        )

        assert set(response['capabilities']) == set(capabilities)
        assert response['keyName'] == key_name

        return self.FULL_APPLICATION_KEY_CLASS.from_create_response(response)

    def delete_key(self, application_key: BaseApplicationKey):  # type: ignore
        return super().delete_key(application_key)  # type: ignore

    def delete_key_by_id(self, application_key_id: str) -> ApplicationKey:  # type: ignore
        return super().delete_key_by_id(application_key_id)  # type: ignore

    def list_keys(  # type: ignore
        self, start_application_key_id: str | None = None
    ) -> Generator[ApplicationKey, None, None]:
        return super().list_keys(start_application_key_id)  # type: ignore

    def get_key(self, key_id: str) -> ApplicationKey | None:  # type: ignore
        return super().get_key(key_id)  # type: ignore

    def check_bucket_name_restrictions(self, bucket_name: str):
        self._check_bucket_restrictions('bucketName', bucket_name)

    def check_bucket_id_restrictions(self, bucket_id: str):
        self._check_bucket_restrictions('bucketId', bucket_id)

    def _check_bucket_restrictions(self, key, value):
        allowed = self.account_info.get_allowed()
        allowed_bucket_identifier = allowed[key]

        if allowed_bucket_identifier is not None:
            if allowed_bucket_identifier != value:
                raise RestrictedBucket(allowed_bucket_identifier)

    def _populate_bucket_cache_from_key(self):
        # If the key is restricted to the bucket, pre-populate the cache with it
        try:
            allowed = self.account_info.get_allowed()
        except MissingAccountData:
            return

        allowed_bucket_id = allowed.get('bucketId')
        if allowed_bucket_id is None:
            return

        allowed_bucket_name = allowed.get('bucketName')

        # If we have bucketId set we still need to check bucketName. If the bucketName is None,
        # it means that the bucketId belongs to a bucket that was already removed.
        if allowed_bucket_name is None:
            raise RestrictedBucketMissing()

        self.cache.save_bucket(self.BUCKET_CLASS(self, allowed_bucket_id, name=allowed_bucket_name))
