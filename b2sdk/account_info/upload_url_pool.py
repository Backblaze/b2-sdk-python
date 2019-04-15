######################################################################
#
# File: b2sdk/account_info/upload_url_pool.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod
import collections
import threading

from .abstract import AbstractAccountInfo


class UploadUrlPool(object):
    """
    For each key (either a bucket id or large file id), holds a pool
    of (url, auth_token) pairs, with thread-safe methods to add and
    remove them.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pool = collections.defaultdict(list)

    def put(self, key, url, auth_token):
        """
        Adds the url and auth token to the pool for the given key.

        :param key: bucket ID or large file ID
        :type param: str
        :param url: bucket or file URL
        :type url: str
        :param auth_token: authentication token
        :type auth_token: str
        """
        with self._lock:
            pair = (url, auth_token)
            self._pool[key].append(pair)

    def take(self, key):
        """
        Returns (url, auth_token) if one is available, or (None, None) if not.

        :param key: bucket ID or large file ID
        :type param: str
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
        Remove an intem from the pool by key

        :param key: bucket ID or large file ID
        :type param: str
        """
        with self._lock:
            if key in self._pool:
                del self._pool[key]


class UrlPoolAccountInfo(AbstractAccountInfo):
    """
    Holder for all account-related information that needs to be kept
    between API calls, and between invocations of the command-line
    tool.  This includes: account ID, application key, auth tokens,
    API URL, download URL, and uploads URLs.

    This concrete implementation uses an instance of UploadUrlPool
    as an underlying storage
    """

    def __init__(self):
        super(UrlPoolAccountInfo, self).__init__()
        self._reset_upload_pools()

    @abstractmethod
    def clear(self):
        """
        Remove all stored information
        """
        self._reset_upload_pools()
        return super(UrlPoolAccountInfo, self).clear()

    def _reset_upload_pools(self):
        self._bucket_uploads = UploadUrlPool()
        self._large_file_uploads = UploadUrlPool()

    # bucket upload url
    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        """
        Add an (upload_url, upload_auth_token) pair to the pool available for
        the bucket.

        :param bucket_id: a bucket ID
        :type bucket_id: str
        :param upload_url: an upload URL
        :type upload_url: str
        :param upload_auth_token: an upload authentication token
        :type upload_auth_token: str
        :rtype: tuple
        """
        self._bucket_uploads.put(bucket_id, upload_url, upload_auth_token)

    def clear_bucket_upload_data(self, bucket_id):
        """
        Removes all upload URLs for the given bucket.

        :param bucket_id: a bucket ID
        :type bucket_id: str
        """
        self._bucket_uploads.clear_for_key(bucket_id)

    def take_bucket_upload_url(self, bucket_id):
        """
        Returns a pair (upload_url, upload_auth_token) that has been removed
        from the pool for this bucket, or (None, None) if there are no more
        left.

        :param bucket_id: a bucket ID
        :type bucket_id: str
        :rtype: tuple
        """
        return self._bucket_uploads.take(bucket_id)

    # large file upload url
    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        """
        Put large file upload URL into a pool

        :param file_id: a file ID
        :type file_id: str
        :param upload_url: an upload URL
        :type upload_url: str
        :param upload_auth_token: an upload authentication token
        :type upload_auth_token: str
        """
        self._large_file_uploads.put(file_id, upload_url, upload_auth_token)

    def take_large_file_upload_url(self, file_id):
        """
        Take large file upload URL from a pool

        :param file_id: a file ID
        :type file_id: str
        """
        return self._large_file_uploads.take(file_id)

    def clear_large_file_upload_urls(self, file_id):
        """
        Clear a pool of URLs for a given file ID

        :param file_id: a file ID
        :type file_id: str
        """
        self._large_file_uploads.clear_for_key(file_id)
