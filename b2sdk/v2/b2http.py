######################################################################
#
# File: b2sdk/v2/b2http.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import _v3 as v3
from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from .exception import BucketIdNotFound


# Overridden to retain old-style BadRequest exception in case of a bad bucket id
class B2Http(v3.B2Http):
    @classmethod
    def _translate_errors(cls, fcn, post_params=None):
        try:
            return super()._translate_errors(fcn, post_params)
        except v3BucketIdNotFound as e:
            raise BucketIdNotFound(e.bucket_id)
