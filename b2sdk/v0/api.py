######################################################################
#
# File: b2sdk/v0/api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from .bucket import Bucket, BucketFactory
from b2sdk import v1


class B2Api(v1.B2Api):
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)

    def delete_bucket(self, bucket):
        """
        Delete the chosen bucket.

        For legacy reasons it returns whatever server sends in response,
        but API user should not rely on the response: if it doesn't raise
        an exception, it means that the operation was a success.

        :param b2sdk.v1.Bucket bucket: a :term:`bucket` to delete
        """
        account_id = self.account_info.get_account_id()
        return self.session.delete_bucket(account_id, bucket.id_)
