######################################################################
#
# File: b2sdk/account_info/in_memory.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .exception import MissingAccountData
from .upload_url_pool import UrlPoolAccountInfo

from functools import wraps


def _raise_missing_if_result_is_none(function):
    """
    Raise MissingAccountData if function's result is None
    """

    @wraps(function)
    def inner(*args, **kwargs):
        assert function.__name__.startswith('get_')
        result = function(*args, **kwargs)
        if result is None:
            # *magic*: assumes that it is a "get_field_name"
            raise MissingAccountData(function.__name__[4:])
        return result

    return inner


class InMemoryAccountInfo(UrlPoolAccountInfo):
    """
    Holder for all account-related information that needs to be kept
    between API calls, and between invocations of the command-line
    tool.  This includes: account ID, application key ID, application key,
    auth tokens, API URL, download URL, and uploads URLs.
    """

    def __init__(self, *args, **kwargs):
        super(InMemoryAccountInfo, self).__init__(*args, **kwargs)
        self._clear_in_memory_account_fields()

    def clear(self):
        """
        Remove all stored information
        """
        self._clear_in_memory_account_fields()
        return super(InMemoryAccountInfo, self).clear()

    def _clear_in_memory_account_fields(self):
        self._account_id = None
        self._application_key_id = None
        self._allowed = None
        self._api_url = None
        self._application_key = None
        self._auth_token = None
        self._buckets = {}
        self._download_url = None
        self._minimum_part_size = None
        self._realm = None

    def _set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        minimum_part_size,
        application_key,
        realm,
        allowed,
        application_key_id,
    ):
        self._account_id = account_id
        self._application_key_id = application_key_id
        self._auth_token = auth_token
        self._api_url = api_url
        self._download_url = download_url
        self._minimum_part_size = minimum_part_size
        self._application_key = application_key
        self._realm = realm
        self._allowed = allowed

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        """
        Removes all previous name-to-id mappings and stores new ones.

        :param name_id_iterable: a list of tuples of the form (name, id)
        :type name_id_iterable: list
        """
        self._buckets = dict(name_id_iterable)

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        """
        Looks up the bucket ID for a given bucket name.

        :param bucket_name: a bucket name
        :type bucket_name: str
        :return bucket ID or None:
        :rtype: str, None
        """
        return self._buckets.get(bucket_name)

    def save_bucket(self, bucket):
        """
        Remembers the ID for a bucket name.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        """
        self._buckets[bucket.name] = bucket.id_

    def remove_bucket_name(self, bucket_name):
        """
        Removes one entry from the bucket name cache.

        :param bucket_name: a bucket name
        :type bucket_name: str
        """
        if bucket_name in self._buckets:
            del self._buckets[bucket_name]

    @_raise_missing_if_result_is_none
    def get_account_id(self):
        """
        Returns account_id or raises MissingAccountData exception

        :rtype: str
        """
        return self._account_id

    @_raise_missing_if_result_is_none
    def get_application_key_id(self):
        """ 
        Returns the application key ID used to authenticate

        :rtype: str
        """
        return self._application_key_id

    @_raise_missing_if_result_is_none
    def get_account_auth_token(self):
        """ 
        Return account_auth_token or raises MissingAccountData exception

        :rtype: str
        """
        return self._auth_token

    @_raise_missing_if_result_is_none
    def get_api_url(self):
        """ 
        Returns api_url or raises MissingAccountData exception

        :rtype: str
        """
        return self._api_url

    @_raise_missing_if_result_is_none
    def get_application_key(self):
        """
        Returns application_key or raises MissingAccountData exception

        :rtype: str
        """
        return self._application_key

    @_raise_missing_if_result_is_none
    def get_download_url(self):
        """
        Returns download_url or raises MissingAccountData exception

        :rtype: str
        """
        return self._download_url

    @_raise_missing_if_result_is_none
    def get_minimum_part_size(self):
        """
        Return the minimum number of bytes in a part of a large file

        :return: number of bytes
        :rtype: int
        """
        return self._minimum_part_size

    @_raise_missing_if_result_is_none
    def get_realm(self):
        """
        Returns realm or raises MissingAccountData exception

        :rtype: str
        """
        return self._realm

    @_raise_missing_if_result_is_none
    def get_allowed(self):
        """
        An 'allowed' dict, as returned by b2_authorize_account.
        Never None; for account info that was saved before 'allowed' existed,
        returns DEFAULT_ALLOWED.

        :rtype: dict
        """
        return self._allowed
