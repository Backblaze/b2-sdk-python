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

from b2sdk import _v3 as v3
from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from .bucket import Bucket, BucketFactory
from .exception import BucketIdNotFound
from .session import B2Session
from .transfer import DownloadManager, UploadManager
from .file_version import FileVersionFactory
from .large_file import LargeFileServices


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

    # Legacy init in case something depends on max_workers defaults = 10
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('max_upload_workers', 10)
        kwargs.setdefault('max_copy_workers', 10)
        super().__init__(*args, **kwargs)

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