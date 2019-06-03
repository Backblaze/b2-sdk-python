######################################################################
#
# File: b2sdk/v0/api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .bucket import Bucket, BucketFactory
from b2sdk.v1 import B2Api


class B2Api(B2Api):
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
