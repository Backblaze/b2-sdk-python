######################################################################
#
# File: b2sdk/v0/bucket.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.v1 import Bucket, BucketFactory


class Bucket(Bucket):
    pass

class BucketFactory(BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
