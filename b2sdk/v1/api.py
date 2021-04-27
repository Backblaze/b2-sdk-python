######################################################################
#
# File: b2sdk/v1/api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .bucket import Bucket, BucketFactory
from b2sdk import _v2 as v2


# Overridden to use v1.Bucket
class B2Api(v2.B2Api):
    BUCKET_FACTORY_CLASS = staticmethod(BucketFactory)
    BUCKET_CLASS = staticmethod(Bucket)
