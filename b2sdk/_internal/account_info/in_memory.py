######################################################################
#
# File: b2sdk/_internal/account_info/in_memory.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from functools import wraps

from .exception import MissingAccountData
from .upload_url_pool import UrlPoolAccountInfo


def _raise_missing_if_result_is_none(function):
    """
    Raise MissingAccountData if function's result is None.
    """

    @wraps(function)
    def inner(*args, **kwargs):
        assert function.__name__.startswith('get_')
        result = function(*args, **kwargs)
        if result is None:
            # assumes that it is a "get_field_name"
            raise MissingAccountData(function.__name__[4:])
        return result

    return inner


class InMemoryAccountInfo(UrlPoolAccountInfo):
    """
    *AccountInfo* which keeps all data in memory.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._clear_in_memory_account_fields()

    def clear(self):
        self._clear_in_memory_account_fields()
        return super().clear()

    def _clear_in_memory_account_fields(self):
        self._account_id = None
        self._application_key_id = None
        self._allowed = None
        self._api_url = None
        self._application_key = None
        self._auth_token = None
        self._buckets = {}
        self._download_url = None
        self._recommended_part_size = None
        self._absolute_minimum_part_size = None
        self._realm = None
        self._s3_api_url = None

    def _set_auth_data(
        self, account_id, auth_token, api_url, download_url, recommended_part_size,
        absolute_minimum_part_size, application_key, realm, s3_api_url, allowed, application_key_id
    ):
        self._account_id = account_id
        self._application_key_id = application_key_id
        self._auth_token = auth_token
        self._api_url = api_url
        self._download_url = download_url
        self._absolute_minimum_part_size = absolute_minimum_part_size
        self._recommended_part_size = recommended_part_size
        self._application_key = application_key
        self._realm = realm
        self._s3_api_url = s3_api_url
        self._allowed = allowed

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        self._buckets = dict(name_id_iterable)

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        return self._buckets.get(bucket_name)

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        for name, cached_id_ in self._buckets.items():
            if cached_id_ == bucket_id:
                return name
        return None

    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        return [(name, id_) for name, id_ in self._buckets.items()]

    def save_bucket(self, bucket):
        self._buckets[bucket.name] = bucket.id_

    def remove_bucket_name(self, bucket_name):
        if bucket_name in self._buckets:
            del self._buckets[bucket_name]

    @_raise_missing_if_result_is_none
    def get_account_id(self):
        return self._account_id

    @_raise_missing_if_result_is_none
    def get_application_key_id(self):
        return self._application_key_id

    @_raise_missing_if_result_is_none
    def get_account_auth_token(self):
        return self._auth_token

    @_raise_missing_if_result_is_none
    def get_api_url(self):
        return self._api_url

    @_raise_missing_if_result_is_none
    def get_application_key(self):
        return self._application_key

    @_raise_missing_if_result_is_none
    def get_download_url(self):
        return self._download_url

    @_raise_missing_if_result_is_none
    def get_recommended_part_size(self):
        return self._recommended_part_size

    @_raise_missing_if_result_is_none
    def get_absolute_minimum_part_size(self):
        return self._absolute_minimum_part_size

    @_raise_missing_if_result_is_none
    def get_realm(self):
        return self._realm

    @_raise_missing_if_result_is_none
    def get_allowed(self):
        return self._allowed

    @_raise_missing_if_result_is_none
    def get_s3_api_url(self):
        return self._s3_api_url
