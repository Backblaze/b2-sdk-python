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

import json

from b2sdk import _v3
from .exception import MissingAccountData


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
    def get_allowed(self):
        """
        Return 'allowed' dictionary info.
        Example:

        .. code-block:: python

            {
                "bucketId": null,
                "bucketName": null,
                "capabilities": [
                    "listKeys",
                    "writeKeys"
                ],
                "namePrefix": null
            }

        The 'allowed' column was not in the original schema, so it may be NULL.

        :rtype: dict
        """
        allowed_json = self._get_account_info_or_raise('allowed')
        if allowed_json is None:
            return self.DEFAULT_ALLOWED

        allowed = json.loads(allowed_json)

        # convert a multi-bucket key to a single bucket

        if 'bucketIds' in allowed:
            bucket_ids = allowed.pop('bucketIds')
            if bucket_ids and len(bucket_ids) > 1:
                raise MissingAccountData(
                    'Multi-bucket keys cannot be used with the current sdk version'
                )

            allowed['bucketId'] = bucket_ids[0] if bucket_ids else None

            bucket_names = allowed.pop('bucketNames')
            allowed['bucketName'] = bucket_names[0] if bucket_names else None

        return allowed


class StubAccountInfo(_OldAllowedMixin, _v3.StubAccountInfo):
    pass
