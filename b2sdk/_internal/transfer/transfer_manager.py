######################################################################
#
# File: b2sdk/_internal/transfer/transfer_manager.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations


class TransferManager:
    """
    Base class for manager classes (copy, upload, download)
    """

    def __init__(self, services, **kwargs):
        self.services = services
        super().__init__(**kwargs)
