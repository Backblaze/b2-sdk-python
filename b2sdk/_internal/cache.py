######################################################################
#
# File: b2sdk/_internal/cache.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from b2sdk._internal.account_info.abstract import AbstractAccountInfo


class AbstractCache(metaclass=ABCMeta):
    def clear(self):
        self.set_bucket_name_cache(tuple())

    @abstractmethod
    def get_bucket_id_or_none_from_bucket_name(self, name):
        pass

    @abstractmethod
    def get_bucket_name_or_none_from_allowed(self):
        pass

    @abstractmethod
    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        pass

    @abstractmethod
    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        """
        List buckets in the cache.

        :return: list of tuples (bucket_name, bucket_id)
        """

    @abstractmethod
    def save_bucket(self, bucket):
        pass

    @abstractmethod
    def set_bucket_name_cache(self, buckets):
        pass

    def _name_id_iterator(self, buckets):
        return ((bucket.name, bucket.id_) for bucket in buckets)


class DummyCache(AbstractCache):
    """
    A cache that does nothing.
    """

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return None

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        return None

    def get_bucket_name_or_none_from_allowed(self):
        return None

    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        return []

    def save_bucket(self, bucket):
        pass

    def set_bucket_name_cache(self, buckets):
        pass


class InMemoryCache(AbstractCache):
    """
    A cache that stores the information in memory.
    """

    def __init__(self):
        self.name_id_map = {}
        self.bucket_name = None

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return self.name_id_map.get(name)

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        for name, cached_id_ in self.name_id_map.items():
            if cached_id_ == bucket_id:
                return name
        return None

    def get_bucket_name_or_none_from_allowed(self):
        return self.bucket_name

    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        return sorted(tuple(item) for item in self.name_id_map.items())

    def save_bucket(self, bucket):
        self.name_id_map[bucket.name] = bucket.id_

    def set_bucket_name_cache(self, buckets):
        self.name_id_map = dict(self._name_id_iterator(buckets))


class AuthInfoCache(AbstractCache):
    """
    A cache that stores data persistently in StoredAccountInfo.
    """

    def __init__(self, info: AbstractAccountInfo):
        self.info = info

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return self.info.get_bucket_id_or_none_from_bucket_name(name)

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id) -> str | None:
        return self.info.get_bucket_name_or_none_from_bucket_id(bucket_id)

    def get_bucket_name_or_none_from_allowed(self):
        return self.info.get_bucket_name_or_none_from_allowed()

    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        return self.info.list_bucket_names_ids()

    def save_bucket(self, bucket):
        self.info.save_bucket(bucket)

    def set_bucket_name_cache(self, buckets):
        self.info.refresh_entire_bucket_name_cache(self._name_id_iterator(buckets))
