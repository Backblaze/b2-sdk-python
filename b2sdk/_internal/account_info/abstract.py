######################################################################
#
# File: b2sdk/_internal/account_info/abstract.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import abstractmethod

from b2sdk._internal.account_info import exception
from b2sdk._internal.raw_api import ALL_CAPABILITIES
from b2sdk._internal.utils import B2TraceMetaAbstract, limit_trace_arguments


class AbstractAccountInfo(metaclass=B2TraceMetaAbstract):
    """
    Abstract class for a holder for all account-related information
    that needs to be kept between API calls and between invocations of the program.

    This includes: account ID, application key ID, application key,
    auth tokens, API URL, download URL, and uploads URLs.

    This class must be THREAD SAFE because it may be used by multiple
    threads running in the same Python process. It also needs to be
    safe against multiple processes running at the same time.
    """

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
        Return a list of all possible capabilities.

        :rtype: list
        """
        return ALL_CAPABILITIES

    @abstractmethod
    def clear(self):
        """
        Remove all stored information.
        """

    @abstractmethod
    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        """
        List buckets in the cache.

        :return: list of tuples (bucket_name, bucket_id)
        """
        pass

    @abstractmethod
    @limit_trace_arguments(only=['self'])
    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        """
        Remove all previous name-to-id mappings and stores new ones.

        :param iterable name_id_iterable: an iterable of tuples of the form (name, id)
        """

    @abstractmethod
    def remove_bucket_name(self, bucket_name):
        """
        Remove one entry from the bucket name cache.

        :param str bucket_name: a bucket name
        """

    @abstractmethod
    def save_bucket(self, bucket):
        """
        Remember the ID for the given bucket name.

        :param b2sdk.v2.Bucket bucket: a Bucket object
        """

    @abstractmethod
    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        """
        Look up the bucket ID for the given bucket name.

        :param str bucket_name: a bucket name
        :return bucket ID or None:
        :rtype: str, None
        """

    @abstractmethod
    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        """
        Look up the bucket name for the given bucket id.
        """

    @abstractmethod
    def clear_bucket_upload_data(self, bucket_id):
        """
        Remove all upload URLs for the given bucket.

        :param str bucket_id: a bucket ID
        """

    def is_same_key(self, application_key_id, realm):
        """
        Check whether cached application key is the same as the one provided.

        :param str application_key_id: application key ID
        :param str realm: authorization realm
        :rtype: bool
        """
        try:
            return self.get_application_key_id() == application_key_id and self.get_realm() == realm
        except exception.MissingAccountData:
            return False

    def is_same_account(self, account_id: str, realm: str) -> bool:
        """
        Check whether cached account is the same as the one provided.

        :param str account_id: account ID
        :param str realm: authorization realm
        :rtype: bool
        """
        try:
            return self.get_account_id() == account_id and self.get_realm() == realm
        except exception.MissingAccountData:
            return False

    def is_master_key(self) -> bool:
        application_key_id = self.get_application_key_id()
        account_id = self.get_account_id()
        new_style_master_key_suffix = '0000000000'
        if account_id == application_key_id:
            return True  # old style
        if len(application_key_id
              ) == (3 + len(account_id) + len(new_style_master_key_suffix)):  # 3 for cluster id
            # new style
            if application_key_id.endswith(account_id + new_style_master_key_suffix):
                return True
        return False

    @abstractmethod
    def get_account_id(self):
        """
        Return account ID or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @abstractmethod
    def get_application_key_id(self):
        """
        Return the application key ID used to authenticate.

        :rtype: str
        """

    @abstractmethod
    def get_account_auth_token(self):
        """
        Return account_auth_token or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @abstractmethod
    def get_api_url(self):
        """
        Return api_url or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @abstractmethod
    def get_application_key(self):
        """
        Return application_key or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @abstractmethod
    def get_download_url(self):
        """
        Return download_url or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @abstractmethod
    def get_realm(self):
        """
        Return realm or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @abstractmethod
    def get_recommended_part_size(self):
        """
        Return the recommended number of bytes in a part of a large file.

        :return: number of bytes
        :rtype: int
        """

    @abstractmethod
    def get_absolute_minimum_part_size(self):
        """
        Return the absolute minimum number of bytes in a part of a large file.

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

    @abstractmethod
    def get_s3_api_url(self):
        """
        Return s3_api_url or raises :class:`~b2sdk.v2.exception.MissingAccountData` exception.

        :rtype: str
        """

    @limit_trace_arguments(
        only=[
            'self',
            'api_url',
            'download_url',
            'recommended_part_size',
            'absolute_minimum_part_size',
            'realm',
            's3_api_url',
        ]
    )
    def set_auth_data(
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
        """
        Check permission correctness and stores the results of ``b2_authorize_account``.

        The allowed structure is the one returned by ``b2_authorize_account``, e.g.

        .. code-block:: python

           {
             "absoluteMinimumPartSize": 5000000,
             "accountId": "YOUR_ACCOUNT_ID",
             "allowed": {
               "bucketId": "BUCKET_ID",
               "bucketName": "BUCKET_NAME",
               "capabilities": [
                 "listBuckets",
                 "listFiles",
                 "readFiles",
                 "shareFiles",
                 "writeFiles",
                 "deleteFiles"
               ],
               "namePrefix": null
             },
             "apiUrl": "https://apiNNN.backblazeb2.com",
             "authorizationToken": "4_0022623512fc8f80000000001_0186e431_d18d02_acct_tH7VW03boebOXayIc43-sxptpfA=",
             "downloadUrl": "https://f002.backblazeb2.com",
             "recommendedPartSize": 100000000,
             "s3ApiUrl": "https://s3.us-west-NNN.backblazeb2.com"
           }

        For keys with bucket restrictions, the name of the bucket is looked
        up and stored as well.  The console_tool does everything by bucket name, so it's convenient
        to have the restricted bucket name handy.

        :param str account_id: user account ID
        :param str auth_token: user authentication token
        :param str api_url: an API URL
        :param str download_url: path download URL
        :param int recommended_part_size: recommended size of a file part
        :param int absolute_minimum_part_size: minimum size of a file part
        :param str application_key: application key
        :param str realm: a realm to authorize account in
        :param dict allowed: the structure to use for old account info that was saved without 'allowed'
        :param str application_key_id: application key ID
        :param str s3_api_url: S3-compatible API URL

        .. versionchanged:: 0.1.5
           `account_id_or_app_key_id` renamed to `application_key_id`
        """
        if allowed is None:
            allowed = self.DEFAULT_ALLOWED
        assert self.allowed_is_valid(allowed)

        self._set_auth_data(
            account_id, auth_token, api_url, download_url, recommended_part_size,
            absolute_minimum_part_size, application_key, realm, s3_api_url, allowed,
            application_key_id
        )

    @classmethod
    def allowed_is_valid(cls, allowed):
        """
        Make sure that all of the required fields are present, and that
        bucketId is set if bucketName is.

        If the bucketId is for a bucket that no longer exists, or the
        capabilities do not allow for listBuckets, then we will not have a bucketName.

        :param dict allowed: the structure to use for old account info that was saved without 'allowed'
        :rtype: bool
        """
        return (
            ('bucketId' in allowed) and ('bucketName' in allowed) and
            ((allowed['bucketId'] is not None) or (allowed['bucketName'] is None)) and
            ('capabilities' in allowed) and ('namePrefix' in allowed)
        )

    @abstractmethod
    def _set_auth_data(
        self, account_id, auth_token, api_url, download_url, recommended_part_size,
        absolute_minimum_part_size, application_key, realm, s3_api_url, allowed, application_key_id
    ):
        """
        Actually store the auth data.  Can assume that 'allowed' is present and valid.

        All of the information returned by ``b2_authorize_account`` is saved, because all of it is
        needed at some point.
        """

    @abstractmethod
    def take_bucket_upload_url(self, bucket_id):
        """
        Return a pair (upload_url, upload_auth_token) that has been removed
        from the pool for this bucket, or (None, None) if there are no more
        left.

        :param str bucket_id: a bucket ID
        :rtype: tuple
        """

    @abstractmethod
    @limit_trace_arguments(only=['self', 'bucket_id'])
    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        """
        Add an (upload_url, upload_auth_token) pair to the pool available for
        the bucket.

        :param str bucket_id: a bucket ID
        :param str upload_url: an upload URL
        :param str upload_auth_token: an upload authentication token
        :rtype: tuple
        """

    @abstractmethod
    @limit_trace_arguments(only=['self'])
    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        """
        Put a large file upload URL into a pool.

        :param str file_id: a file ID
        :param str upload_url: an upload URL
        :param str upload_auth_token: an upload authentication token
        """
        pass

    @abstractmethod
    def take_large_file_upload_url(self, file_id):
        """
        Take the chosen large file upload URL from the pool.

        :param str file_id: a file ID
        """
        pass

    @abstractmethod
    def clear_large_file_upload_urls(self, file_id):
        """
        Clear the pool of URLs for a given file ID.

        :param str file_id: a file ID
        """
        pass
