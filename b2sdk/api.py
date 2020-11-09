######################################################################
#
# File: b2sdk/api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Any, Dict
from .bucket import Bucket, BucketFactory
from .exception import NonExistentBucket, RestrictedBucket
from .file_version import FileIdAndName
from .large_file.services import LargeFileServices
from .raw_api import API_VERSION
from .session import B2Session
from .transfer import (
    CopyManager,
    DownloadManager,
    Emerger,
    UploadManager,
)
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

    def __init__(self, session, max_upload_workers=10, max_copy_workers=10):
        """
        Initialize Services object using given session.

        :param b2sdk.v1.Session session:
        :param int max_upload_workers: a number of upload threads
        :param int max_copy_workers: a number of copy threads
        """
        self.session = session
        self.large_file = LargeFileServices(self)
        self.download_manager = DownloadManager(self)
        self.upload_manager = UploadManager(self, max_upload_workers=max_upload_workers)
        self.copy_manager = CopyManager(self, max_copy_workers=max_copy_workers)
        self.emerger = Emerger(self)


class B2Api(metaclass=B2TraceMeta):
    """
    Provide file-level access to B2 services.

    While :class:`b2sdk.v1.B2RawApi` provides direct access to the B2 web APIs, this
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

    def __init__(
        self,
        account_info=None,
        cache=None,
        raw_api=None,
        max_upload_workers=10,
        max_copy_workers=10
    ):
        """
        Initialize the API using the given account info.

        :param account_info: an instance of :class:`~b2sdk.v1.UrlPoolAccountInfo`,
                      or any custom class derived from
                      :class:`~b2sdk.v1.AbstractAccountInfo`
                      To learn more about Account Info objects, see here
                      :class:`~b2sdk.v1.SqliteAccountInfo`

        :param cache: an instance of the one of the following classes:
                      :class:`~b2sdk.cache.DummyCache`, :class:`~b2sdk.cache.InMemoryCache`,
                      :class:`~b2sdk.cache.AuthInfoCache`,
                      or any custom class derived from :class:`~b2sdk.cache.AbstractCache`
                      It is used by B2Api to cache the mapping between bucket name and bucket ids.
                      default is :class:`~b2sdk.cache.DummyCache`

        :param raw_api: an instance of one of the following classes:
                        :class:`~b2sdk.raw_api.B2RawApi`, :class:`~b2sdk.raw_simulator.RawSimulator`,
                        or any custom class derived from :class:`~b2sdk.raw_api.AbstractRawApi`
                        It makes network-less unit testing simple by using :class:`~b2sdk.raw_simulator.RawSimulator`,
                        in tests and :class:`~b2sdk.raw_api.B2RawApi` in production.
                        default is :class:`~b2sdk.raw_api.B2RawApi`

        :param int max_upload_workers: a number of upload threads, default is 10
        :param int max_copy_workers: a number of copy threads, default is 10
        """
        self.session = B2Session(account_info=account_info, cache=cache, raw_api=raw_api)
        self.services = Services(
            self.session,
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
            :class:`~b2sdk.raw_api.B2RawApi` attribute is deprecated.
            :class:`~b2sdk.session.B2Session` expose all :class:`~b2sdk.raw_api.B2RawApi` methods now."""
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
        self, name, bucket_type, bucket_info=None, cors_rules=None, lifecycle_rules=None
    ):
        """
        Create a bucket.

        :param str name: bucket name
        :param str bucket_type: a bucket type, could be one of the following values: ``"allPublic"``, ``"allPrivate"``
        :param dict bucket_info: additional bucket info to store with the bucket
        :param dict cors_rules: bucket CORS rules to store with the bucket
        :param dict lifecycle_rules: bucket lifecycle rules to store with the bucket
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
            lifecycle_rules=lifecycle_rules
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

    def download_file_by_id(self, file_id, download_dest, progress_listener=None, range_=None):
        """
        Download a file with the given ID.

        :param str file_id: a file ID
        :param download_dest: an instance of the one of the following classes: \
        :class:`~b2sdk.v1.DownloadDestLocalFile`,\
        :class:`~b2sdk.v1.DownloadDestBytes`,\
        :class:`~b2sdk.v1.DownloadDestProgressWrapper`,\
        :class:`~b2sdk.v1.PreSeekedDownloadDest`,\
        or any sub class of :class:`~b2sdk.v1.AbstractDownloadDestination`
        :param progress_listener: an instance of the one of the following classes: \
        :class:`~b2sdk.v1.PartProgressReporter`,\
        :class:`~b2sdk.v1.TqdmProgressListener`,\
        :class:`~b2sdk.v1.SimpleProgressListener`,\
        :class:`~b2sdk.v1.DoNothingProgressListener`,\
        :class:`~b2sdk.v1.ProgressListenerForTest`,\
        :class:`~b2sdk.v1.SyncFileReporter`,\
        or any sub class of :class:`~b2sdk.v1.AbstractProgressListener`
        :param list range_: a list of two integers, the first one is a start\
        position, and the second one is the end position in the file
        :return: context manager that returns an object that supports iter_content()
        """
        url = self.session.get_download_url_by_id(file_id)
        return self.services.download_manager.download_file_from_url(
            url, download_dest, progress_listener, range_
        )

    def get_bucket_by_id(self, bucket_id):
        """
        Return a bucket object with a given ID.  Unlike ``get_bucket_by_name``, this method does not need to make any API calls.

        :param str bucket_id: a bucket ID
        :return: a Bucket object
        :rtype: b2sdk.v1.Bucket
        """
        return self.BUCKET_CLASS(self, bucket_id)

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
        self.check_bucket_restrictions(bucket_name)

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

    def list_buckets(self, bucket_name=None):
        """
        Call ``b2_list_buckets`` and return a list of buckets.

        When no bucket name is specified, returns *all* of the buckets
        in the account.  When a bucket name is given, returns just that
        bucket.  When authorized with an :term:`application key` restricted to
        one :term:`bucket`, you must specify the bucket name, or the request
        will be unauthorized.

        :param str bucket_name: the name of the one bucket to return
        :rtype: list[b2sdk.v1.Bucket]
        """
        # Give a useful warning if the current application key does not
        # allow access to the named bucket.
        self.check_bucket_restrictions(bucket_name)

        account_id = self.account_info.get_account_id()
        self.check_bucket_restrictions(bucket_name)

        response = self.session.list_buckets(account_id, bucket_name=bucket_name)
        buckets = self.BUCKET_FACTORY_CLASS.from_api_response(self, response)

        if bucket_name is not None:
            # If a bucket_name is specified we don't clear the cache because the other buckets could still
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
    def cancel_large_file(self, file_id):
        """
        Cancel a large file upload.

        :param str file_id: a file ID
        :rtype: None
        """
        return self.services.large_file.cancel_large_file(file_id)

    def delete_file_version(self, file_id, file_name):
        """
        Permanently and irrevocably delete one version of a file.

        :param str file_id: a file ID
        :param str file_name: a file name
        :rtype: FileIdAndName
        """
        # filename argument is not first, because one day it may become optional
        response = self.session.delete_file_version(file_id, file_name)
        assert response['fileId'] == file_id
        assert response['fileName'] == file_name
        return FileIdAndName(file_id, file_name)

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
        self.check_bucket_restrictions(bucket_name)
        return '%s/file/%s/%s' % (
            self.account_info.get_download_url(), bucket_name, b2_url_encode(file_name)
        )

    # keys
    def create_key(
        self,
        capabilities,
        key_name,
        valid_duration_seconds=None,
        bucket_id=None,
        name_prefix=None,
    ):
        """
        Create a new :term:`application key`.

        :param list capabilities: a list of capabilities
        :param str key_name: a name of a key
        :param int,None valid_duration_seconds: key auto-expire time after it is created, in seconds, or ``None`` to not expire
        :param str,None bucket_id: a bucket ID to restrict the key to, or ``None`` to not restrict
        :param str,None name_prefix: a remote filename prefix to restrict the key to or ``None`` to not restrict
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

        return response

    def delete_key(self, application_key_id):
        """
        Delete :term:`application key`.

        :param str application_key_id: an :term:`application key ID`
        """

        response = self.session.delete_key(application_key_id=application_key_id)
        return response

    def list_keys(self, start_application_key_id=None):
        """
        List application keys.

        :param str,None start_application_key_id: an :term:`application key ID` to start from or ``None`` to start from the beginning
        """
        account_id = self.account_info.get_account_id()

        return self.session.list_keys(
            account_id, max_key_count=1000, start_application_key_id=start_application_key_id
        )

    # other
    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """
        Legacy interface which just returns whatever remote API returns.

        .. todo::
            get_file_info() should return a File with .delete(), copy(), rename(), read() and so on

        :param str file_id: the id of the file who's info will be retrieved.
        :return: The parsed response
        :rtype: dict
        """
        return self.session.get_file_info_by_id(file_id)

    def check_bucket_restrictions(self, bucket_name):
        """
        Check to see if the allowed field from authorize-account has a bucket restriction.

        If it does, checks if the bucket_name for a given api call matches that.
        If not, it raises a :py:exc:`b2sdk.v1.exception.RestrictedBucket` error.

        :param str bucket_name: a bucket name
        :raises b2sdk.v1.exception.RestrictedBucket: if the account is not allowed to use this bucket
        """
        allowed = self.account_info.get_allowed()
        allowed_bucket_name = allowed['bucketName']

        if allowed_bucket_name is not None:
            if allowed_bucket_name != bucket_name:
                raise RestrictedBucket(allowed_bucket_name)
