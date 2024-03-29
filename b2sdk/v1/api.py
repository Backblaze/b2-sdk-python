######################################################################
#
# File: b2sdk/v1/api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from typing import Any, overload

from .download_dest import AbstractDownloadDestination
from b2sdk import v2
from b2sdk._internal.api import Services
from .account_info import AbstractAccountInfo
from .bucket import Bucket, BucketFactory, download_file_and_return_info_dict
from .cache import AbstractCache
from .file_version import FileVersionInfo, FileVersionInfoFactory, file_version_info_from_id_and_name
from .session import B2Session


# override to use legacy no-request method of creating a bucket from bucket_id and retain `check_bucket_restrictions`
# public API method
# and to use v1.Bucket
# and to retain cancel_large_file return type
# and to retain old style download_file_by_id signature (allowing for the new one as well) and exception
# and to retain old style get_file_info return type
# and to accept old-style raw_api argument
# and to retain old style create_key, delete_key and list_keys interfaces and behaviour
class B2Api(v2.B2Api):
    SESSION_CLASS = staticmethod(B2Session)
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionInfoFactory)

    def __init__(
        self,
        account_info: AbstractAccountInfo | None = None,
        cache: AbstractCache | None = None,
        raw_api: v2.B2RawHTTPApi = None,
        max_upload_workers: int = 10,
        max_copy_workers: int = 10,
        api_config: v2.B2HttpApiConfig | None = None,
    ):
        """
        Initialize the API using the given account info.

        :param account_info: To learn more about Account Info objects, see here
                      :class:`~b2sdk.v1.SqliteAccountInfo`

        :param cache: It is used by B2Api to cache the mapping between bucket name and bucket ids.
                      default is :class:`~b2sdk._internal.cache.DummyCache`

        :param max_upload_workers: a number of upload threads
        :param max_copy_workers: a number of copy threads
        :param raw_api:
        :param api_config:
        """
        self.session = self.SESSION_CLASS(
            account_info=account_info,
            cache=cache,
            raw_api=raw_api,
            api_config=api_config,
        )
        self.file_version_factory = self.FILE_VERSION_FACTORY_CLASS(self)
        self.download_version_factory = self.DOWNLOAD_VERSION_FACTORY_CLASS(self)
        self.services = Services(
            self,
            max_upload_workers=max_upload_workers,
            max_copy_workers=max_copy_workers,
        )

    def get_file_info(self, file_id: str) -> dict[str, Any]:
        """
        Gets info about file version.

        :param str file_id: the id of the file.
        """
        return self.session.get_file_info_by_id(file_id)

    def get_bucket_by_id(self, bucket_id):
        """
        Return a bucket object with a given ID.  Unlike ``get_bucket_by_name``, this method does not need to make any API calls.

        :param str bucket_id: a bucket ID
        :return: a Bucket object
        :rtype: b2sdk.v1.Bucket
        """
        return self.BUCKET_CLASS(self, bucket_id)

    def check_bucket_restrictions(self, bucket_name):
        """
        Check to see if the allowed field from authorize-account has a bucket restriction.

        If it does, checks if the bucket_name for a given api call matches that.
        If not, it raises a :py:exc:`b2sdk.v1.exception.RestrictedBucket` error.

        :param str bucket_name: a bucket name
        :raises b2sdk.v1.exception.RestrictedBucket: if the account is not allowed to use this bucket
        """
        self.check_bucket_name_restrictions(bucket_name)

    def cancel_large_file(self, file_id: str) -> FileVersionInfo:
        file_id_and_name = super().cancel_large_file(file_id)
        return file_version_info_from_id_and_name(file_id_and_name, self)

    @overload
    def download_file_by_id(
        self,
        file_id: str,
        download_dest: AbstractDownloadDestination,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ) -> dict:
        ...

    @overload
    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ) -> v2.DownloadedFile:
        ...

    def download_file_by_id(
        self,
        file_id: str,
        download_dest: AbstractDownloadDestination | None = None,
        progress_listener: v2.AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: v2.EncryptionSetting | None = None,
    ):
        """
        Download a file with the given ID.

        :param file_id: a file ID
        :param download_dest: an instance of the one of the following classes: \
        :class:`~b2sdk.v1.DownloadDestLocalFile`,\
        :class:`~b2sdk.v1.DownloadDestBytes`,\
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
        :param range_: a list of two integers, the first one is a start\
        position, and the second one is the end position in the file
        :param encryption: encryption settings (``None`` if unknown)
        """
        downloaded_file = super().download_file_by_id(
            file_id=file_id,
            progress_listener=progress_listener,
            range_=range_,
            encryption=encryption,
        )
        if download_dest is not None:
            try:
                return download_file_and_return_info_dict(downloaded_file, download_dest, range_)
            except ValueError as ex:
                if ex.args == ('no strategy suitable for download was found!',):
                    raise AssertionError('no strategy suitable for download was found!')
                raise
        else:
            return downloaded_file

    def list_keys(self, start_application_key_id=None) -> dict:
        """
        List application keys. Perform a single request and return at most ``self.DEFAULT_LIST_KEY_COUNT`` keys, as
        well as the value to supply to the next call as ``start_application_key_id``, if not all keys were retrieved.

        :param start_application_key_id: an :term:`application key ID` to start from or ``None`` to start from the beginning
        """
        account_id = self.account_info.get_account_id()

        return self.session.list_keys(
            account_id,
            max_key_count=self.DEFAULT_LIST_KEY_COUNT,
            start_application_key_id=start_application_key_id
        )

    def create_key(
        self,
        capabilities: list[str],
        key_name: str,
        valid_duration_seconds: int | None = None,
        bucket_id: str | None = None,
        name_prefix: str | None = None,
    ):
        return super().create_key(
            capabilities=capabilities,
            key_name=key_name,
            valid_duration_seconds=valid_duration_seconds,
            bucket_id=bucket_id,
            name_prefix=name_prefix,
        ).as_dict()

    def delete_key(self, application_key_id):
        return super().delete_key_by_id(application_key_id).as_dict()

    def get_key(self, key_id: str) -> dict | None:
        keys = self.list_keys(start_application_key_id=key_id)['keys']
        return next((key for key in keys if key['applicationKeyId'] == key_id), None)
