######################################################################
#
# File: b2sdk/_internal/account_info/upload_url_pool.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import collections
import threading
from abc import abstractmethod

from .abstract import AbstractAccountInfo


class UploadUrlPool:
    """
    For each key (either a bucket id or large file id), hold a pool
    of (url, auth_token) pairs.

    .. note:
        This class is thread-safe.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pool = collections.defaultdict(list)

    def put(self, key, url, auth_token):
        """
        Add the url and auth token to the pool for the given key.

        :param str key: bucket ID or large file ID
        :param str url: bucket or file URL
        :param str auth_token: authentication token
        """
        with self._lock:
            pair = (url, auth_token)
            self._pool[key].append(pair)

    def take(self, key):
        """
        Return a (url, auth_token) if one is available, or (None, None) if not.

        :param str key: bucket ID or large file ID
        :rtype: tuple
        """
        with self._lock:
            pair_list = self._pool[key]
            if pair_list:
                return pair_list.pop()
            else:
                return (None, None)

    def clear_for_key(self, key):
        """
        Remove an item from the pool by key.

        :param str key: bucket ID or large file ID
        """
        with self._lock:
            if key in self._pool:
                del self._pool[key]


class UrlPoolAccountInfo(AbstractAccountInfo):
    """
    Implement part of :py:class:`AbstractAccountInfo` for upload URL pool management
    with a simple, key-value storage, such as :py:class:`b2sdk.v2.UploadUrlPool`.
    """
    # staticmethod is necessary here to avoid the first argument binding to the first argument (like ``partial(fun, arg)``)
    BUCKET_UPLOAD_POOL_CLASS = staticmethod(
        UploadUrlPool
    )  #: A url pool class to use for small files.
    LARGE_FILE_UPLOAD_POOL_CLASS = staticmethod(
        UploadUrlPool
    )  #: A url pool class to use for large files.

    def __init__(self):
        super().__init__()
        self._reset_upload_pools()

    @abstractmethod
    def clear(self):
        self._reset_upload_pools()
        return super().clear()

    def _reset_upload_pools(self):
        self._bucket_uploads = self.BUCKET_UPLOAD_POOL_CLASS()
        self._large_file_uploads = self.LARGE_FILE_UPLOAD_POOL_CLASS()

    # bucket upload url
    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        self._bucket_uploads.put(bucket_id, upload_url, upload_auth_token)

    def clear_bucket_upload_data(self, bucket_id):
        self._bucket_uploads.clear_for_key(bucket_id)

    def take_bucket_upload_url(self, bucket_id):
        return self._bucket_uploads.take(bucket_id)

    # large file upload url
    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        self._large_file_uploads.put(file_id, upload_url, upload_auth_token)

    def take_large_file_upload_url(self, file_id):
        return self._large_file_uploads.take(file_id)

    def clear_large_file_upload_urls(self, file_id):
        self._large_file_uploads.clear_for_key(file_id)
