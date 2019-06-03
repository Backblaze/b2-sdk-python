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
    def list_file_names(self, start_filename=None, max_entries=None):
        """
        legacy interface which just returns whatever remote API returns
        """
        return self.api.session.list_file_names(self.id_, start_filename, max_entries)

    def list_file_versions(self, start_filename=None, start_file_id=None, max_entries=None):
        """
        legacy interface which just returns whatever remote API returns
        """
        return self.api.session.list_file_versions(
            self.id_, start_filename, start_file_id, max_entries
        )


class BucketFactory(BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
