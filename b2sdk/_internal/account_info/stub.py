######################################################################
#
# File: b2sdk/_internal/account_info/stub.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import collections
import threading

from .abstract import AbstractAccountInfo


class StubAccountInfo(AbstractAccountInfo):
    def __init__(self):
        self._clear_stub_account_fields()

    def clear(self):
        self._clear_stub_account_fields()

    def _clear_stub_account_fields(self):
        self.application_key = None
        self.buckets = {}
        self.account_id = None
        self.allowed = None
        self.api_url = None
        self.auth_token = None
        self.download_url = None
        self.absolute_minimum_part_size = None
        self.recommended_part_size = None
        self.realm = None
        self.application_key_id = None
        self._large_file_uploads = collections.defaultdict(list)
        self._large_file_uploads_lock = threading.Lock()

    def clear_bucket_upload_data(self, bucket_id):
        if bucket_id in self.buckets:
            del self.buckets[bucket_id]

    def _set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        recommended_part_size,
        absolute_minimum_part_size,
        application_key,
        realm,
        s3_api_url,
        allowed,
        application_key_id,
    ):
        self.account_id = account_id
        self.auth_token = auth_token
        self.api_url = api_url
        self.download_url = download_url
        self.recommended_part_size = recommended_part_size
        self.absolute_minimum_part_size = absolute_minimum_part_size
        self.application_key = application_key
        self.realm = realm
        self.s3_api_url = s3_api_url
        self.allowed = allowed
        self.application_key_id = application_key_id

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        self.buckets = {}

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        return None

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        return None

    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        return list((bucket.bucket_name, bucket.id_) for bucket in self.buckets.values())

    def save_bucket(self, bucket):
        self.buckets[bucket.id_] = bucket

    def remove_bucket_name(self, bucket_name):
        pass

    def take_bucket_upload_url(self, bucket_id):
        return (None, None)

    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        pass

    def get_account_id(self):
        return self.account_id

    def get_application_key_id(self):
        return self.application_key_id

    def get_account_auth_token(self):
        return self.auth_token

    def get_api_url(self):
        return self.api_url

    def get_application_key(self):
        return self.application_key

    def get_download_url(self):
        return self.download_url

    def get_recommended_part_size(self):
        return self.recommended_part_size

    def get_absolute_minimum_part_size(self):
        return self.absolute_minimum_part_size

    def get_realm(self):
        return self.realm

    def get_allowed(self):
        return self.allowed

    def get_bucket_upload_data(self, bucket_id):
        return self.buckets.get(bucket_id, (None, None))

    def get_s3_api_url(self):
        return self.s3_api_url

    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        with self._large_file_uploads_lock:
            self._large_file_uploads[file_id].append((upload_url, upload_auth_token))

    def take_large_file_upload_url(self, file_id):
        with self._large_file_uploads_lock:
            upload_urls = self._large_file_uploads.get(file_id, [])
            if not upload_urls:
                return (None, None)
            else:
                return upload_urls.pop()

    def clear_large_file_upload_urls(self, file_id):
        with self._large_file_uploads_lock:
            if file_id in self._large_file_uploads:
                del self._large_file_uploads[file_id]
