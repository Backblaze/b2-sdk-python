######################################################################
#
# File: b2sdk/account_info/abstract.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod

import six

from b2sdk import version_utils
from b2sdk.raw_api import ALL_CAPABILITIES
from b2sdk.utils import B2TraceMetaAbstract, limit_trace_arguments


@six.add_metaclass(B2TraceMetaAbstract)
class AbstractAccountInfo(object):
    """
    Holder for all account-related information that needs to be kept
    between API calls, and between invocations of the command-line
    tool.  This includes: account ID, application key ID,  application key,
    auth tokens, API URL, download URL, and uploads URLs.

    This class must be THREAD SAFE because it may be used by multiple
    threads running in the same Python process.  It also needs to be
    safe against multiple processes running at the same time.
    """

    REALM_URLS = {
        'production': 'https://api.backblazeb2.com',
        'dev': 'http://api.backblazeb2.xyz:8180',
        'staging': 'https://api.backblaze.net',
    }

    # The 'allowed' structure to use for old account info that was saved without 'allowed'.
    DEFAULT_ALLOWED = dict(
        bucketId=None,
        bucketName=None,
        capabilities=ALL_CAPABILITIES,
        namePrefix=None,
    )

    @classmethod
    def all_capabilities(cls):
        """
        Return a list of all possible capabilities

        :rtype: list
        """
        return cls.ALL_CAPABILITIES

    @abstractmethod
    def clear(self):
        """
        Removes all stored information
        """

    @abstractmethod
    @limit_trace_arguments(only=['self'])
    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        """
        Removes all previous name-to-id mappings and stores new ones.

        :param name_id_iterable: a list of tuples of the form (name, id)
        :type name_id_iterable: list
        """

    @abstractmethod
    def remove_bucket_name(self, bucket_name):
        """
        Removes one entry from the bucket name cache.

        :param bucket_name: a bucket name
        :type bucket_name: str
        """

    @abstractmethod
    def save_bucket(self, bucket):
        """
        Remembers the ID for a bucket name.

        :param bucket: a Bucket object
        :type bucket: b2sdk.bucket.Bucket
        """

    @abstractmethod
    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        """
        Looks up the bucket ID for a given bucket name.

        :param bucket_name: a bucket name
        :type bucket_name: str
        :return bucket ID or None:
        :rtype: str, None
        """

    @abstractmethod
    def clear_bucket_upload_data(self, bucket_id):
        """
        Removes all upload URLs for the given bucket.

        :param bucket_id: a bucket ID
        :type bucket_id: str
        """

    @abstractmethod
    def get_account_id(self):
        """
        Returns account ID or raises MissingAccountData exception

        :rtype: str
        """

    @abstractmethod
    def get_application_key_id(self):
        """
        Returns the application key ID used to authenticate

        :rtype: str
        """

    @version_utils.rename_method(get_application_key_id, '0.1.5', '0.2.0')
    def get_account_id_or_app_key_id(self):
        """
        Returns the application key ID used to authenticate

        :rtype: str

        .. deprecated:: 0.1.6
           Use :func:`get_application_key_id` instead.
        """
        return self.get_application_key_id()

    @abstractmethod
    def get_account_auth_token(self):
        """
        Returns account_auth_token or raises MissingAccountData exception

        :rtype: str
        """

    @abstractmethod
    def get_api_url(self):
        """
        Returns api_url or raises MissingAccountData exception

        :rtype: str
        """

    @abstractmethod
    def get_application_key(self):
        """
        Returns application_key or raises MissingAccountData exception

        :rtype: str
        """

    @abstractmethod
    def get_download_url(self):
        """
        Returns download_url or raises MissingAccountData exception

        :rtype: str
        """

    @abstractmethod
    def get_realm(self):
        """
        Returns realm or raises MissingAccountData exception

        :rtype: str
        """

    @abstractmethod
    def get_minimum_part_size(self):
        """
        Return the minimum number of bytes in a part of a large file

        :return: number of bytes
        :rtype: int
        """

    @abstractmethod
    def get_allowed(self):
        """
        An 'allowed' dict, as returned by ``b2_authorize_account``.
        Never ``None``; for account info that was saved before 'allowed' existed,
        returns :attr:`DEFAULT_ALLOWED`.

        :rtype: dict
        """

    @version_utils.rename_argument(
        'account_id_or_app_key_id',
        'application_key_id',
        '0.1.5',
        '0.2.0',
    )
    @limit_trace_arguments(only=['self', 'api_url', 'download_url', 'minimum_part_size', 'realm'])
    def set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        minimum_part_size,
        application_key,
        realm,
        allowed=None,
        application_key_id=None,
    ):
        """
        Stores the results of ``b2_authorize_account``.

        All of the information returned by ``b2_authorize_account`` is saved, because all of it is
        needed by some command.

        The allowed structure is the one returned ``b2_authorize_account``, with the addition of
        a bucketName field.  For keys with bucket restrictions, the name of the bucket is looked
        up and stored, too.  The console_tool does everything by bucket name, so it's convenient
        to have the restricted bucket name handy.

        :param account_id: user account ID
        :type account_id: str
        :param auth_token: user authentication token
        :type auth_token: str
        :param api_url: an API URL
        :type api_url: str
        :param download_url: path download URL
        :type download_url: str
        :param minimum_part_size: minimum size of the file part
        :type minimum_part_size: int
        :param application_key: application key
        :type application_key: str
        :param realm: a realm to authorize account in
        :type realm: str
        :param allowed: the structure to use for old account info that was saved without 'allowed'
        :type allowed: dict
        :param application_key_id: application key ID
        :type application_key_id: str

        .. versionchanged:: 0.1.5
           `account_id_or_app_key_id` renamed to `get_application_key_id`
        """
        if allowed is None:
            allowed = self.DEFAULT_ALLOWED
        assert self.allowed_is_valid(allowed)
        self._set_auth_data(
            account_id,
            auth_token,
            api_url,
            download_url,
            minimum_part_size,
            application_key,
            realm,
            allowed,
            application_key_id,
        )

    @classmethod
    def allowed_is_valid(cls, allowed):
        """
        Makes sure that all of the required fields are present, and that
        bucketId is set if bucketName is.

        If the bucketId is for a bucket that no longer exists, or the
        capabilities do not allow listBuckets, then we won't have a bucketName.

        :param allowed: the structure to use for old account info that was saved without 'allowed'
        :type allowed: dict
        :rtype: bool
        """
        return (
            ('bucketId' in allowed) and ('bucketName' in allowed) and
            ((allowed['bucketId'] is not None) or (allowed['bucketName'] is None)) and
            ('capabilities' in allowed) and ('namePrefix' in allowed)
        )

    # TODO: make a decorator for set_auth_data()
    @abstractmethod
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
        """
        Stores the auth data.  Can assume that 'allowed' is present and valid.
        """

    @abstractmethod
    def take_bucket_upload_url(self, bucket_id):
        """
        Returns a pair (upload_url, upload_auth_token) that has been removed
        from the pool for this bucket, or (None, None) if there are no more
        left.

        :param bucket_id: a bucket ID
        :type bucket_id: str
        :rtype: tuple
        """

    @abstractmethod
    @limit_trace_arguments(only=['self', 'bucket_id'])
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

    @abstractmethod
    @limit_trace_arguments(only=['self'])
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
        pass

    @abstractmethod
    def take_large_file_upload_url(self, file_id):
        """
        Take large file upload URL from a pool

        :param file_id: a file ID
        :type file_id: str
        """
        pass

    @abstractmethod
    def clear_large_file_upload_urls(self, file_id):
        """
        Clear a pool of URLs for a given file ID

        :param file_id: a file ID
        :type file_id: str
        """
        pass
