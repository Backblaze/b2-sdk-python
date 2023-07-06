######################################################################
#
# File: b2sdk/v2/account_info.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
from b2sdk import _v3


class AbstractAccountInfo(_v3.AbstractAccountInfo):
    def list_bucket_names_ids(self):
        return []  # Removed @abstractmethod decorator
