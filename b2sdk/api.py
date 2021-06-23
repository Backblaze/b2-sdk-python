######################################################################
#
# File: b2sdk/api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional, Tuple, List, Generator

from .account_info.abstract import AbstractAccountInfo
from .api_config import B2HttpApiConfig, DEFAULT_HTTP_API_CONFIG
from .application_key import ApplicationKey, BaseApplicationKey, FullApplicationKey
from .cache import AbstractCache
from .bucket import Bucket, BucketFactory
from .encryption.setting import EncryptionSetting
from .exception import BucketIdNotFound, NonExistentBucket, RestrictedBucket
from .file_lock import FileRetentionSetting, LegalHold
from .file_version import DownloadVersionFactory, FileIdAndName, FileVersion, FileVersionFactory
from .large_file.services import LargeFileServices
from .raw_api import API_VERSION
from .progress import AbstractProgressListener
from .session import B2Session
from .transfer import (
    CopyManager,
    DownloadManager,
    Emerger,
    UploadManager,
)
from .transfer.inbound.downloaded_file import DownloadedFile
from .utils import B2TraceMeta, b2_url_encode, limit_trace_arguments


def url_for_api(info, api_name):
    """
    Return URL for an API endpoint.

    :param info: account info
    :param str api_nam:
    :rtype: str
    """
    if api_name in ['b2_download_file_by_id']:
        base = info.get_download_url()
    else:
        base = info.get_api_url()
    return '%s/b2api/%s/%s' % (base, API_VERSION, api_name)


class Services(object):
    """ Gathers objects that provide high level logic over raw api usage. """

    def __init__(self, api, max_upload_workers=10, max_copy_workers=10):
        """
        Initialize Services object using given session.

        :param b2sdk.v1.B2Api api:
        :param int max_upload_workers: a number of upload threads
        :param int max_copy_workers: a number of copy threads
        """
        self.api = api
        self.session = api.session
        self.large_file = LargeFileServices(self)
        self.download_manager = DownloadManager(self)
        self.upload_manager = UploadManager(self, max_upload_workers=max_upload_workers)
        self.copy_manager = CopyManager(self, max_copy_workers=max_copy_workers)
        self.emerger = Emerger(self)


class B2Api(metaclass=B2TraceMeta):
    """
    Provide file-level access to B2 services.

    While :class:`b2sdk.v1.B2RawHTTPApi` provides direct access to the B2 web APIs, this
    class handles several things that simplify the task of uploading
    and downloading files:

    - re-acquires authorization tokens when they expire
    - retrying uploads when an upload URL is busy
    - breaking large files into parts
    - emulating a directory structure (B2 buckets are flat)

    Adds an object-oriented layer on top of the raw API, so that
    buckets and files returned are Python objects with accessor
    methods.

    The class also keeps a cache of information needed to access the
    service, such as auth tokens and upload URLs.
    """
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
    SESSION_CLASS = staticmethod(B2Session)
    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionFactory)
    DOWNLOAD_VERSION_FACTORY_CLASS = staticmethod(DownloadVersionFactory)
    DEFAULT_LIST_KEY_COUNT = 1000

    def __init__(
        self,
        account_info: Optional[AbstractAccountInfo] = None,
        cache: Optional[AbstractCache] = None,
        max_upload_workers: int = 10,
        max_copy_workers: int = 10,
        api_config: B2HttpApiConfig = DEFAULT_HTTP_API_CONFIG,
    ):
        """
        Initialize the API using the given account info.

        :param account_info: To learn more about Account Info objects, see here
                      :class:`~b2sdk.v1.SqliteAccountInfo`

        :param cache: It is used by B2Api to cache the mapping between bucket name and bucket ids.
                      default is :class:`~b2sdk.cache.DummyCache`

        :param max_upload_workers: a number of upload threads
        :param max_copy_workers: a number of copy threads
        :param api_config:
        """
        self.session = self.SESSION_CLASS(
            account_info=account_info, cache=cache, api_config=api_config
        )
        self.file_version_factory = self.FILE_VERSION_FACTORY_CLASS(self)
        self.download_version_factory = self.DOWNLOAD_VERSION_FACTORY_CLASS(self)
        self.services = Services(
            self,
            max_upload_workers=max_upload_workers,
            max_copy_workers=max_copy_workers,
        )

    @property
    def account_info(self):
        return self.session.account_info

    @property
    def cache(self):
        return self.session.cache

    @property
    def raw_api(self):
        """
        .. warning::
            :class:`~b2sdk.raw_api.B2RawHTTPApi` attribute is deprecated.
            :class:`~b2sdk.session.B2Session` expose all :class:`~b2sdk.raw_api.B2RawHTTPApi` methods now."""
        return self.session.raw_api

    def authorize_automatically(self):
        """
        Perform automatic account authorization, retrieving all account data
        from account info object passed during initialization.
        """
        return self.session.authorize_automatically()

    @limit_trace_arguments(only=('self', 'realm'))
    def authorize_account(self, realm, application_key_id, application_key):
        """
        Perform account authorization.

        :param str realm: a realm to authorize account in (usually just "production")
        :param str application_key_id: :term:`application key ID`
        :param str application_key: user's :term:`application key`
        """
        self.session.authorize_account(realm, application_key_id, application_key)

    def get_account_id(self):
        """
        Return the account ID.

        :rtype: str
        """
        return self.account_info.get_account_id()

    # buckets

    def create_bucket(
        self,
        name,
        bucket_type,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        default_server_side_encryption: Optional[EncryptionSetting] = None,
        is_file_lock_enabled: Optional[bool] = None,
    ):
        """
        Create a bucket.

        :param str name: bucket name
        :param str bucket_type: a bucket type, could be one of the following values: ``"allPublic"``, ``"allPrivate"``
        :param dict bucket_info: additional bucket info to store with the bucket
        :param dict cors_rules: bucket CORS rules to store with the bucket
        :param dict lifecycle_rules: bucket lifecycle rules to store with the bucket
        :param b2sdk.v1.EncryptionSetting default_server_side_encryption: default server side encryption settings (``None`` if unknown)
        :param bool is_file_lock_enabled: boolean value specifies whether bucket is File Lock-enabled
        :return: a Bucket object
        :rtype: b2sdk.v1.Bucket
        """
        account_id = self.account_info.get_account_id()

        response = self.session.create_bucket(
            account_id,
            name,
            bucket_type,
            bucket_info=bucket_info,
            cors_rules=cors_rules,
            lifecycle_rules=lifecycle_rules,
            default_server_side_encryption=default_server_side_encryption,
            is_file_lock_enabled=is_file_lock_enabled,
        )
        bucket = self.BUCKET_FACTORY_CLASS.from_api_bucket_dict(self, response)
        assert name == bucket.name, 'API created a bucket with different name\
                                     than requested: %s != %s' % (name, bucket.name)
        assert bucket_type == bucket.type_, 'API created a bucket with different type\
                                             than requested: %s != %s' % (
            bucket_type, bucket.type_
        )
        self.cache.save_bucket(bucket)
        return bucket

    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: Optional[AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[EncryptionSetting] = None,
    ) -> DownloadedFile:
        """
        Download a file with the given ID.

        :param str file_id: a file ID
        :param progress_listener: a progress listener object to use, or ``None`` to not track progress
        :param range_: a list of two integers, the first one is a start\
        position, and the second one is the end position in the file
        :param encryption: encryption settings (``None`` if unknown)
        """
        url = self.session.get_download_url_by_id(file_id)
        return self.services.download_manager.download_file_from_url(
            url,
            progress_listener,
            range_,
            encryption,
        )

    def update_file_retention(
        self,
        file_id: str,
        file_name: str,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ):
        self.session.update_file_retention(
            file_id,
            file_name,
            file_retention,
            bypass_governance,
        )

    def update_file_legal_hold(
        self,
        file_id: str,
        file_name: str,
        legal_hold: LegalHold,
    ):
        self.session.update_file_legal_hold(
            file_id,
            file_name,
            legal_hold,
        )

    def get_bucket_by_id(self, bucket_id: str) -> Bucket:
        """
        Return the Bucket matching the given bucket_id.
        :raises b2sdk.v1.exception.NonExistentBucket: if the bucket does not exist in the account
        """
        # Give a useful warning if the current application key does not
        # allow access to bucket.
        self.check_bucket_id_restrictions(bucket_id)

        # First, try the cache.
        bucket_name = self.cache.get_bucket_name_or_none_from_bucket_id(bucket_id)
        if bucket_name is not None:
            return self.BUCKET_CLASS(self, bucket_id, name=bucket_name)

        # Second, ask the service
        for bucket in self.list_buckets(bucket_id=bucket_id):
            assert bucket.id_ == bucket_id
            return bucket

        # There is no such bucket.
        raise BucketIdNotFound(bucket_id)

    def get_bucket_by_name(self, bucket_name):
        """
        Return the Bucket matching the given bucket_name.

        :param str bucket_name: the name of the bucket to return
        :return: a Bucket object
        :rtype: b2sdk.v1.Bucket
        :raises b2sdk.v1.exception.NonExistentBucket: if the bucket does not exist in the account
        """
        # Give a useful warning if the current application key does not
        # allow access to the named bucket.
        self.check_bucket_name_restrictions(bucket_name)

        # First, try the cache.
        id_ = self.cache.get_bucket_id_or_none_from_bucket_name(bucket_name)
        if id_ is not None:
            return self.BUCKET_CLASS(self, id_, name=bucket_name)

        # Second, ask the service
        for bucket in self.list_buckets(bucket_name=bucket_name):
            assert bucket.name.lower() == bucket_name.lower()
            return bucket

        # There is no such bucket.
        raise NonExistentBucket(bucket_name)

    def delete_bucket(self, bucket):
        """
        Delete a chosen bucket.

        :param b2sdk.v1.Bucket bucket: a :term:`bucket` to delete
        :rtype: None
        """
        account_id = self.account_info.get_account_id()
        self.session.delete_bucket(account_id, bucket.id_)

    def list_buckets(self, bucket_name=None, bucket_id=None):
        """
        Call ``b2_list_buckets`` and return a list of buckets.

        When no bucket name nor ID is specified, returns *all* of the buckets
        in the account.  When a bucket name or ID is given, returns just that
        bucket.  When authorized with an :term:`application key` restricted to
        one :term:`bucket`, you must specify the bucket name or bucket id, or
        the request will be unauthorized.

        :param str bucket_name: the name of the one bucket to return
        :param str bucket_id: the ID of the one bucket to return
        :rtype: list[b2sdk.v1.Bucket]
        """
        # Give a useful warning if the current application key does not
        # allow access to the named bucket.
        if bucket_name is not None and bucket_id is not None:
            raise ValueError('Provide either bucket_name or bucket_id, not both')
        if bucket_id:
            self.check_bucket_id_restrictions(bucket_id)
        else:
            self.check_bucket_name_restrictions(bucket_name)

        account_id = self.account_info.get_account_id()

        response = self.session.list_buckets(
            account_id, bucket_name=bucket_name, bucket_id=bucket_id
        )
        buckets = self.BUCKET_FACTORY_CLASS.from_api_response(self, response)

        if bucket_name or bucket_id:
            # If a bucket_name or bucket_id is specified we don't clear the cache because the other buckets could still
            # be valid. So we save the one bucket returned from the list_buckets call.
            for bucket in buckets:
                self.cache.save_bucket(bucket)
        else:
            # Otherwise we want to clear the cache and save the buckets returned from list_buckets
            # since we just got a new list of all the buckets for this account.
            self.cache.set_bucket_name_cache(buckets)
        return buckets

    def list_parts(self, file_id, start_part_number=None, batch_size=None):
        """
        Generator that yields a :py:class:`b2sdk.v1.Part` for each of the parts that have been uploaded.

        :param str file_id: the ID of the large file that is not finished
        :param int start_part_number: the first part number to return; defaults to the first part
        :param int batch_size: the number of parts to fetch at a time from the server
        :rtype: generator
        """
        return self.services.large_file.list_parts(
            file_id, start_part_number=start_part_number, batch_size=batch_size
        )

    # delete/cancel
    def cancel_large_file(self, file_id: str) -> FileIdAndName:
        """
        Cancel a large file upload.
        """
        return self.services.large_file.cancel_large_file(file_id)

    def delete_file_version(self, file_id: str, file_name: str) -> FileIdAndName:
        """
        Permanently and irrevocably delete one version of a file.
        """
        # filename argument is not first, because one day it may become optional
        response = self.session.delete_file_version(file_id, file_name)
        return FileIdAndName.from_cancel_or_delete_response(response)

    # download
    def get_download_url_for_fileid(self, file_id):
        """
        Return a URL to download the given file by ID.

        :param str file_id: a file ID
        """
        url = url_for_api(self.account_info, 'b2_download_file_by_id')
        return '%s?fileId=%s' % (url, file_id)

    def get_download_url_for_file_name(self, bucket_name, file_name):
        """
        Return a URL to download the given file by name.

        :param str bucket_name: a bucket name
        :param str file_name: a file name
        """
        self.check_bucket_name_restrictions(bucket_name)
        return '%s/file/%s/%s' % (
            self.account_info.get_download_url(), bucket_name, b2_url_encode(file_name)
        )

    # keys
    def create_key(
        self,
        capabilities: List[str],
        key_name: str,
        valid_duration_seconds: Optional[int] = None,
        bucket_id: Optional[str] = None,
        name_prefix: Optional[str] = None,
    ):
        """
        Create a new :term:`application key`.

        :param capabilities: a list of capabilities
        :param key_name: a name of a key
        :param valid_duration_seconds: key auto-expire time after it is created, in seconds, or ``None`` to not expire
        :param bucket_id: a bucket ID to restrict the key to, or ``None`` to not restrict
        :param name_prefix: a remote filename prefix to restrict the key to or ``None`` to not restrict
        """
        account_id = self.account_info.get_account_id()

        response = self.session.create_key(
            account_id,
            capabilities=capabilities,
            key_name=key_name,
            valid_duration_seconds=valid_duration_seconds,
            bucket_id=bucket_id,
            name_prefix=name_prefix
        )

        assert set(response['capabilities']) == set(capabilities)
        assert response['keyName'] == key_name

        return FullApplicationKey.from_create_response(response)

    def delete_key(self, application_key: BaseApplicationKey):
        """
        Delete :term:`application key`.

        :param application_key: an :term:`application key`
        """

        return self.delete_key_by_id(application_key.id_)

    def delete_key_by_id(self, application_key_id: str):
        """
        Delete :term:`application key`.

        :param application_key_id: an :term:`application key ID`
        """

        response = self.session.delete_key(application_key_id=application_key_id)
        return ApplicationKey.from_api_response(response)

    def list_keys(self, start_application_key_id: Optional[str] = None
                 ) -> Generator[ApplicationKey, None, None]:
        """
        List application keys. Lazily perform requests to B2 cloud and return all keys.

        :param start_application_key_id: an :term:`application key ID` to start from or ``None`` to start from the beginning
        """
        account_id = self.account_info.get_account_id()

        while True:
            response = self.session.list_keys(
                account_id,
                max_key_count=self.DEFAULT_LIST_KEY_COUNT,
                start_application_key_id=start_application_key_id,
            )
            for entry in response['keys']:
                yield ApplicationKey.from_api_response(entry)

            next_application_key_id = response['nextApplicationKeyId']
            if next_application_key_id is None:
                return
            start_application_key_id = next_application_key_id

    # other
    def get_file_info(self, file_id: str) -> FileVersion:
        """
        Gets info about file version.

        :param str file_id: the id of the file who's info will be retrieved.
        """
        return self.file_version_factory.from_api_response(
            self.session.get_file_info_by_id(file_id)
        )

    def check_bucket_name_restrictions(self, bucket_name: str):
        """
        Check to see if the allowed field from authorize-account has a bucket restriction.

        If it does, checks if the bucket_name for a given api call matches that.
        If not, it raises a :py:exc:`b2sdk.v1.exception.RestrictedBucket` error.

        :raises b2sdk.v1.exception.RestrictedBucket: if the account is not allowed to use this bucket
        """
        self._check_bucket_restrictions('bucketName', bucket_name)

    def check_bucket_id_restrictions(self, bucket_id: str):
        """
        Check to see if the allowed field from authorize-account has a bucket restriction.

        If it does, checks if the bucket_id for a given api call matches that.
        If not, it raises a :py:exc:`b2sdk.v1.exception.RestrictedBucket` error.

        :raises b2sdk.v1.exception.RestrictedBucket: if the account is not allowed to use this bucket
        """
        self._check_bucket_restrictions('bucketId', bucket_id)

    def _check_bucket_restrictions(self, key, value):
        allowed = self.account_info.get_allowed()
        allowed_bucket_identifier = allowed[key]

        if allowed_bucket_identifier is not None:
            if allowed_bucket_identifier != value:
                raise RestrictedBucket(allowed_bucket_identifier)
