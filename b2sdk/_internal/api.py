######################################################################
#
# File: b2sdk/_internal/api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
from collections.abc import Sequence
from contextlib import suppress
from typing import Generator

from .account_info.abstract import AbstractAccountInfo
from .account_info.exception import MissingAccountData
from .api_config import DEFAULT_HTTP_API_CONFIG, B2HttpApiConfig
from .application_key import ApplicationKey, BaseApplicationKey, FullApplicationKey
from .bucket import Bucket, BucketFactory
from .cache import AbstractCache
from .encryption.setting import EncryptionSetting
from .exception import (
    BucketIdNotFound,
    NonExistentBucket,
    RestrictedBucket,
    RestrictedBucketMissing,
)
from .file_lock import FileRetentionSetting, LegalHold
from .file_version import (
    DownloadVersion,
    DownloadVersionFactory,
    FileIdAndName,
    FileVersion,
    FileVersionFactory,
)
from .large_file.services import LargeFileServices
from .progress import AbstractProgressListener
from .raw_api import API_VERSION, LifecycleRule
from .replication.setting import ReplicationConfiguration
from .session import B2Session
from .transfer import (
    CopyManager,
    DownloadManager,
    Emerger,
    UploadManager,
)
from .transfer.inbound.downloaded_file import DownloadedFile
from .utils import B2TraceMeta, b2_url_encode, limit_trace_arguments

logger = logging.getLogger(__name__)


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
    return f'{base}/b2api/{API_VERSION}/{api_name}'


class Services:
    """ Gathers objects that provide high level logic over raw api usage. """
    UPLOAD_MANAGER_CLASS = staticmethod(UploadManager)
    COPY_MANAGER_CLASS = staticmethod(CopyManager)
    DOWNLOAD_MANAGER_CLASS = staticmethod(DownloadManager)
    LARGE_FILE_SERVICES_CLASS = staticmethod(LargeFileServices)

    def __init__(
        self,
        api,
        max_upload_workers: int | None = None,
        max_copy_workers: int | None = None,
        max_download_workers: int | None = None,
        save_to_buffer_size: int | None = None,
        check_download_hash: bool = True,
        max_download_streams_per_file: int | None = None,
    ):
        """
        Initialize Services object using given session.

        :param b2sdk.v2.B2Api api:
        :param max_upload_workers: a number of upload threads
        :param max_copy_workers: a number of copy threads
        :param max_download_workers: maximum number of download threads
        :param save_to_buffer_size: buffer size to use when writing files using DownloadedFile.save_to
        :param check_download_hash: whether to check hash of downloaded files. Can be disabled for files with internal checksums, for example, or to forcefully retrieve objects with corrupted payload or hash value
        :param max_download_streams_per_file: how many streams to use for parallel downloader
        """
        self.api = api
        self.session = api.session
        self.large_file = self.LARGE_FILE_SERVICES_CLASS(self)
        self.upload_manager = self.UPLOAD_MANAGER_CLASS(
            services=self, max_workers=max_upload_workers
        )
        self.copy_manager = self.COPY_MANAGER_CLASS(services=self, max_workers=max_copy_workers)
        assert max_download_streams_per_file is None or max_download_streams_per_file >= 1
        self.download_manager = self.DOWNLOAD_MANAGER_CLASS(
            services=self,
            max_workers=max_download_workers,
            write_buffer_size=save_to_buffer_size,
            check_hash=check_download_hash,
            max_download_streams_per_file=max_download_streams_per_file,
        )
        self.emerger = Emerger(self)


class B2Api(metaclass=B2TraceMeta):
    """
    Provide file-level access to B2 services.

    While :class:`b2sdk.v2.B2RawHTTPApi` provides direct access to the B2 web APIs, this
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
    SERVICES_CLASS = staticmethod(Services)
    DEFAULT_LIST_KEY_COUNT = 1000

    def __init__(
        self,
        account_info: AbstractAccountInfo | None = None,
        cache: AbstractCache | None = None,
        max_upload_workers: int | None = None,
        max_copy_workers: int | None = None,
        api_config: B2HttpApiConfig = DEFAULT_HTTP_API_CONFIG,
        max_download_workers: int | None = None,
        save_to_buffer_size: int | None = None,
        check_download_hash: bool = True,
        max_download_streams_per_file: int | None = None,
    ):
        """
        Initialize the API using the given account info.

        :param account_info: To learn more about Account Info objects, see here
                      :class:`~b2sdk.v2.SqliteAccountInfo`

        :param cache: It is used by B2Api to cache the mapping between bucket name and bucket ids.
                      default is :class:`~b2sdk._internal.cache.DummyCache`

        :param max_upload_workers: a number of upload threads
        :param max_copy_workers: a number of copy threads
        :param api_config:
        :param max_download_workers: maximum number of download threads
        :param save_to_buffer_size: buffer size to use when writing files using DownloadedFile.save_to
        :param check_download_hash: whether to check hash of downloaded files. Can be disabled for files with internal checksums, for example, or to forcefully retrieve objects with corrupted payload or hash value
        :param max_download_streams_per_file: number of streams for parallel download manager
        """
        self.session = self.SESSION_CLASS(
            account_info=account_info, cache=cache, api_config=api_config
        )
        self.api_config = api_config
        self.file_version_factory = self.FILE_VERSION_FACTORY_CLASS(self)
        self.download_version_factory = self.DOWNLOAD_VERSION_FACTORY_CLASS(self)
        self.services = self.SERVICES_CLASS(
            api=self,
            max_upload_workers=max_upload_workers,
            max_copy_workers=max_copy_workers,
            max_download_workers=max_download_workers,
            save_to_buffer_size=save_to_buffer_size,
            check_download_hash=check_download_hash,
            max_download_streams_per_file=max_download_streams_per_file,
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
            :class:`~b2sdk._internal.raw_api.B2RawHTTPApi` attribute is deprecated.
            :class:`~b2sdk._internal.session.B2Session` expose all
            :class:`~b2sdk._internal.raw_api.B2RawHTTPApi` methods now.
        """
        return self.session.raw_api

    def authorize_automatically(self):
        """
        Perform automatic account authorization, retrieving all account data
        from account info object passed during initialization.
        """
        return self.session.authorize_automatically()

    @limit_trace_arguments(only=('self', 'realm'))
    def authorize_account(self, application_key_id, application_key, realm='production'):
        """
        Perform account authorization.

        :param str application_key_id: :term:`application key ID`
        :param str application_key: user's :term:`application key`
        :param str realm: a realm to authorize account in (usually just "production")
        """
        self.session.authorize_account(realm, application_key_id, application_key)
        self._populate_bucket_cache_from_key()

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
        lifecycle_rules: list[LifecycleRule] | None = None,
        default_server_side_encryption: EncryptionSetting | None = None,
        is_file_lock_enabled: bool | None = None,
        replication: ReplicationConfiguration | None = None,
    ) -> Bucket:
        """
        Create a bucket.

        :param str name: bucket name
        :param str bucket_type: a bucket type, could be one of the following values: ``"allPublic"``, ``"allPrivate"``
        :param dict bucket_info: additional bucket info to store with the bucket
        :param dict cors_rules: bucket CORS rules to store with the bucket
        :param lifecycle_rules: bucket lifecycle rules to store with the bucket
        :param b2sdk.v2.EncryptionSetting default_server_side_encryption: default server side encryption settings (``None`` if unknown)
        :param bool is_file_lock_enabled: boolean value specifies whether bucket is File Lock-enabled
        :param b2sdk.v2.ReplicationConfiguration replication: bucket replication rules or ``None``
        :return: a Bucket object
        :rtype: b2sdk.v2.Bucket
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
            replication=replication,
        )
        bucket = self.BUCKET_FACTORY_CLASS.from_api_bucket_dict(self, response)
        assert name == bucket.name, f'API created a bucket with different name than requested: {name} != {name}'
        assert bucket_type == bucket.type_, f'API created a bucket with different type than requested: {bucket_type} != {bucket.type_}'
        self.cache.save_bucket(bucket)
        return bucket

    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
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
    ) -> FileRetentionSetting:
        return FileRetentionSetting.from_server_response(
            self.session.update_file_retention(
                file_id,
                file_name,
                file_retention,
                bypass_governance,
            )
        )

    def update_file_legal_hold(
        self,
        file_id: str,
        file_name: str,
        legal_hold: LegalHold,
    ) -> LegalHold:
        return LegalHold.from_server_response(
            self.session.update_file_legal_hold(
                file_id,
                file_name,
                legal_hold,
            )
        )

    def get_bucket_by_id(self, bucket_id: str) -> Bucket:
        """
        Return the Bucket matching the given bucket_id.
        :raises b2sdk.v2.exception.BucketIdNotFound: if the bucket does not exist in the account
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

    def get_bucket_by_name(self, bucket_name: str) -> Bucket:
        """
        Return the Bucket matching the given bucket_name.

        :param str bucket_name: the name of the bucket to return
        :return: a Bucket object
        :rtype: b2sdk.v2.Bucket
        :raises b2sdk.v2.exception.NonExistentBucket: if the bucket does not exist in the account
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

        :param b2sdk.v2.Bucket bucket: a :term:`bucket` to delete
        :rtype: None
        """
        account_id = self.account_info.get_account_id()
        self.session.delete_bucket(account_id, bucket.id_)

    def list_buckets(self, bucket_name=None, bucket_id=None, *,
                     use_cache: bool = False) -> Sequence[Bucket]:
        """
        Call ``b2_list_buckets`` and return a list of buckets.

        When no bucket name nor ID is specified, returns *all* of the buckets
        in the account.  When a bucket name or ID is given, returns just that
        bucket.  When authorized with an :term:`application key` restricted to
        one :term:`bucket`, you must specify the bucket name or bucket id, or
        the request will be unauthorized.

        :param str bucket_name: the name of the one bucket to return
        :param str bucket_id: the ID of the one bucket to return
        :param bool use_cache: if ``True`` use cached bucket list if available and not empty
        :rtype: list[b2sdk.v2.Bucket]
        """
        # Give a useful warning if the current application key does not
        # allow access to the named bucket.
        if bucket_name is not None and bucket_id is not None:
            raise ValueError('Provide either bucket_name or bucket_id, not both')
        if bucket_id:
            self.check_bucket_id_restrictions(bucket_id)
        else:
            self.check_bucket_name_restrictions(bucket_name)

        if use_cache:
            cached_list = self.cache.list_bucket_names_ids()
            buckets = [
                self.BUCKET_CLASS(self, cache_b_id, name=cached_b_name)
                for cached_b_name, cache_b_id in cached_list if (
                    (bucket_name is None or bucket_name == cached_b_name) and
                    (bucket_id is None or bucket_id == cache_b_id)
                )
            ]
            if buckets:
                logger.debug("Using cached bucket list as it is not empty")
                return buckets

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
        Generator that yields a :py:class:`b2sdk.v2.Part` for each of the parts that have been uploaded.

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

    def delete_file_version(
        self, file_id: str, file_name: str, bypass_governance: bool = False
    ) -> FileIdAndName:
        """
        Permanently and irrevocably delete one version of a file. bypass_governance must be set to true if deleting a
        file version protected by Object Lock governance mode retention settings (unless its retention period expired)
        """
        # filename argument is not first, because one day it may become optional
        response = self.session.delete_file_version(file_id, file_name, bypass_governance)
        return FileIdAndName.from_cancel_or_delete_response(response)

    # download
    def get_download_url_for_fileid(self, file_id):
        """
        Return a URL to download the given file by ID.

        :param str file_id: a file ID
        """
        url = url_for_api(self.account_info, 'b2_download_file_by_id')
        return f'{url}?fileId={file_id}'

    def get_download_url_for_file_name(self, bucket_name, file_name):
        """
        Return a URL to download the given file by name.

        :param str bucket_name: a bucket name
        :param str file_name: a file name
        """
        self.check_bucket_name_restrictions(bucket_name)
        return '{}/file/{}/{}'.format(
            self.account_info.get_download_url(), bucket_name, b2_url_encode(file_name)
        )

    # keys
    def create_key(
        self,
        capabilities: list[str],
        key_name: str,
        valid_duration_seconds: int | None = None,
        bucket_id: str | None = None,
        name_prefix: str | None = None,
    ) -> FullApplicationKey:
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

    def delete_key_by_id(self, application_key_id: str) -> ApplicationKey:
        """
        Delete :term:`application key`.

        :param application_key_id: an :term:`application key ID`
        """

        response = self.session.delete_key(application_key_id=application_key_id)
        return ApplicationKey.from_api_response(response)

    def list_keys(self, start_application_key_id: str | None = None
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

    def get_key(self, key_id: str) -> ApplicationKey | None:
        """
        Gets information about a single key: it's capabilities, prefix, name etc

        Returns `None` if the key does not exist.

        Raises an exception if profile is not permitted to list keys.
        """
        with suppress(StopIteration):
            key = next(self.list_keys(start_application_key_id=key_id))

            # list_keys() may return some other key if `key_id` does not exist;
            # thus manually check that we retrieved the right key
            if key.id_ == key_id:
                return key

    # other
    def get_file_info(self, file_id: str) -> FileVersion:
        """
        Gets info about file version.

        :param str file_id: the id of the file whose info will be retrieved.
        """
        return self.file_version_factory.from_api_response(
            self.session.get_file_info_by_id(file_id)
        )

    def get_file_info_by_name(self, bucket_name: str, file_name: str) -> DownloadVersion:
        """
        Gets info about a file version. Similar to `get_file_info` but 
        takes the bucket name and file name instead of file id.

        :param str bucket_name: The name of the bucket where the file resides.
        :param str file_name: The name of the file whose info will be retrieved.
        """
        bucket = self.get_bucket_by_name(bucket_name)
        return bucket.get_file_info_by_name(file_name)

    def check_bucket_name_restrictions(self, bucket_name: str):
        """
        Check to see if the allowed field from authorize-account has a bucket restriction.

        If it does, checks if the bucket_name for a given api call matches that.
        If not, it raises a :py:exc:`b2sdk.v2.exception.RestrictedBucket` error.

        :raises b2sdk.v2.exception.RestrictedBucket: if the account is not allowed to use this bucket
        """
        self._check_bucket_restrictions('bucketName', bucket_name)

    def check_bucket_id_restrictions(self, bucket_id: str):
        """
        Check to see if the allowed field from authorize-account has a bucket restriction.

        If it does, checks if the bucket_id for a given api call matches that.
        If not, it raises a :py:exc:`b2sdk.v2.exception.RestrictedBucket` error.

        :raises b2sdk.v2.exception.RestrictedBucket: if the account is not allowed to use this bucket
        """
        self._check_bucket_restrictions('bucketId', bucket_id)

    def _check_bucket_restrictions(self, key, value):
        allowed = self.account_info.get_allowed()
        allowed_bucket_identifier = allowed[key]

        if allowed_bucket_identifier is not None:
            if allowed_bucket_identifier != value:
                raise RestrictedBucket(allowed_bucket_identifier)

    def _populate_bucket_cache_from_key(self):
        # If the key is restricted to the bucket, pre-populate the cache with it
        try:
            allowed = self.account_info.get_allowed()
        except MissingAccountData:
            return

        allowed_bucket_id = allowed.get('bucketId')
        if allowed_bucket_id is None:
            return

        allowed_bucket_name = allowed.get('bucketName')

        # If we have bucketId set we still need to check bucketName. If the bucketName is None,
        # it means that the bucketId belongs to a bucket that was already removed.
        if allowed_bucket_name is None:
            raise RestrictedBucketMissing()

        self.cache.save_bucket(self.BUCKET_CLASS(self, allowed_bucket_id, name=allowed_bucket_name))
