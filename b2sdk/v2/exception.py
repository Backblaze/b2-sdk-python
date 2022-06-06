######################################################################
#
# File: b2sdk/v2/exception.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk._v3.exception import *  # noqa

v3BucketIdNotFound = BucketIdNotFound
UnSyncableFilename = UnsupportedFilename


# overridden to retain old style isinstance check and attributes
class BucketIdNotFound(v3BucketIdNotFound, BadRequest):
    def __init__(self, bucket_id):
        super().__init__(bucket_id)
        self.message = 'Bucket with id=%s not found' % (bucket_id,)
        self.code = 'bad_bucket_id'

    def __str__(self):
        return super(BadRequest, self).__str__()
