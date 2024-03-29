######################################################################
#
# File: b2sdk/v2/transfer.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import _v3 as v3
from b2sdk._internal.utils.thread_pool import LazyThreadPool  # noqa: F401


class ThreadPoolMixin(v3.ThreadPoolMixin):
    pass


class DownloadManager(v3.DownloadManager):
    pass


class UploadManager(v3.UploadManager):
    pass
