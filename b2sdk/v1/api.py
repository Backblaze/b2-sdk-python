######################################################################
#
# File: b2sdk/v1/api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v2 as v2
from .bucket import Bucket, BucketFactory
from .file_version import FileVersionInfo, FileVersionInfoFactory, file_version_info_from_id_and_name
from .session import B2Session


# override to use legacy no-request method of creating a bucket from bucket_id and retain `check_bucket_restrictions`
# public API method
# and to use v1.Bucket
# and to retain cancel_large_file return type
class B2Api(v2.B2Api):
    SESSION_CLASS = staticmethod(B2Session)
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionInfoFactory)

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
