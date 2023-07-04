######################################################################
#
# File: b2sdk/v1/replication/monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from dataclasses import dataclass

import b2sdk.v2 as v2

from .. import Bucket
from ..sync.folder import B2Folder


@dataclass
class ReplicationMonitor(v2.ReplicationMonitor):

    # when passing in v1 Bucket objects to ReplicationMonitor,
    # the latter should use v1 B2Folder to correctly use
    # v1 Bucket's interface
    B2_FOLDER_CLASS = B2Folder

    @property
    def destination_bucket(self) -> Bucket:
        destination_api = self.destination_api or self.source_api
        bucket_id = self.rule.destination_bucket_id

        # when using `destination_api.get_bucket_by_id(bucket_id)`,
        # v1 will instantiate the bucket without its name, but we need it,
        # so we use `list_buckets` to actually fetch bucket name
        return destination_api.list_buckets(bucket_id=bucket_id)[0]
