######################################################################
#
# File: b2sdk/_v3/api.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from unittest import mock

from b2sdk import api

from .transfer import UploadManager


class B2Api(api.B2Api):

    # Legacy init in case something depends on max_workers defaults = 10
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('max_upload_workers', 10)
        kwargs.setdefault('max_copy_workers', 10)
        with mock.patch('b2sdk.api.UploadManager', UploadManager):
            super().__init__(*args, **kwargs)
