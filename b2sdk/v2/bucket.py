######################################################################
#
# File: b2sdk/v2/bucket.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v3 as v3
from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from .exception import BucketIdNotFound


# Overridden to raise old style BucketIdNotFound exception
class Bucket(v3.Bucket):
    def get_fresh_state(self) -> 'Bucket':
        try:
            return super().get_fresh_state()
        except v3BucketIdNotFound as e:
            raise BucketIdNotFound(e.bucket_id)


# Overridden to use old style Bucket
class BucketFactory(v3.BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
