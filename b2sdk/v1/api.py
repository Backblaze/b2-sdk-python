######################################################################
#
# File: b2sdk/v1/api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Any, Dict, Optional, overload, Tuple

from .download_dest import AbstractDownloadDestination
from b2sdk import _v2 as v2
from .bucket import Bucket, BucketFactory, download_file_and_return_info_dict
from .file_version import FileVersionInfo, FileVersionInfoFactory, file_version_info_from_id_and_name
from .session import B2Session


# override to use legacy no-request method of creating a bucket from bucket_id and retain `check_bucket_restrictions`
# public API method
# and to use v1.Bucket
# and to retain cancel_large_file return type
# and to retain old style download_file_by_id signature (allowing for the new one as well) and exception
# and to retain old style get_file_info return type
class B2Api(v2.B2Api):
    SESSION_CLASS = staticmethod(B2Session)
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionInfoFactory)

    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """
        Gets info about file version.

        :param str file_id: the id of the file who's info will be retrieved.
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
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
    ) -> dict:
        ...

    @overload
    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
    ) -> v2.DownloadedFile:
        ...

    def download_file_by_id(
        self,
        file_id: str,
        download_dest: Optional[AbstractDownloadDestination] = None,
        progress_listener: Optional[v2.AbstractProgressListener] = None,
        range_: Optional[Tuple[int, int]] = None,
        encryption: Optional[v2.EncryptionSetting] = None,
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
