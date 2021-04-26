######################################################################
#
# File: b2sdk/v1/cache.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from typing import Optional

from b2sdk import _v2 as v2


class AbstractCache(v2.AbstractCache):
    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> Optional[str]:
        return None
        # Removed @abstractmethod decorator
