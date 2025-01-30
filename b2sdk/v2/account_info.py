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


class _OldAllowedMixin:
    DEFAULT_ALLOWED = dict(
        bucketId=None,
        bucketName=None,
        capabilities=_v3.ALL_CAPABILITIES,
        namePrefix=None,
    )

    @classmethod
    def allowed_is_valid(cls, allowed):
        return (
            ('bucketId' in allowed)
            and ('bucketName' in allowed)
            and ((allowed['bucketId'] is not None) or (allowed['bucketName'] is None))
            and ('capabilities' in allowed)
            and ('namePrefix' in allowed)
        )


class AbstractAccountInfo(_OldAllowedMixin, _v3.AbstractAccountInfo):
    def list_bucket_names_ids(self):
        return []  # Removed @abstractmethod decorator


class UrlPoolAccountInfo(_OldAllowedMixin, _v3.UrlPoolAccountInfo):
    pass


class InMemoryAccountInfo(_OldAllowedMixin, _v3.InMemoryAccountInfo):
    pass


class SqliteAccountInfo(_OldAllowedMixin, _v3.SqliteAccountInfo):
    pass


class StubAccountInfo(_OldAllowedMixin, _v3.StubAccountInfo):
    pass
