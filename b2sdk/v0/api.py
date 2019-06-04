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
from b2sdk import v1


class B2Api(v1.B2Api):
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
