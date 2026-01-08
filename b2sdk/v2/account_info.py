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


from copy import copy
from b2sdk import v3
from .exception import MissingAccountData


class _OldAllowedMixin:
    DEFAULT_ALLOWED = dict(
        bucketId=None,
        bucketName=None,
        capabilities=v3.ALL_CAPABILITIES,
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

    @classmethod
    def _convert_allowed_single_to_multi_bucket(cls, allowed):
        new = copy(allowed)

        bucket_id = new.pop('bucketId')
        bucket_name = new.pop('bucketName')

        if bucket_id is not None:
            new['buckets'] = [{'id': bucket_id, 'name': bucket_name}]
        else:
            new['buckets'] = None

        return new

    def get_allowed(self):
        allowed = super().get_allowed()

        if 'buckets' in allowed:
            buckets = allowed.pop('buckets')
            if buckets and len(buckets) > 1:
                raise MissingAccountData(
                    'Multi-bucket keys cannot be used with the current sdk version'
                )

            allowed['bucketId'] = buckets[0]['id'] if buckets else None
            allowed['bucketName'] = buckets[0]['name'] if buckets else None

        return allowed

    def _set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        recommended_part_size,
        absolute_minimum_part_size,
        application_key,
        realm,
        s3_api_url,
        allowed,
        application_key_id,
    ):
        new_allowed = self._convert_allowed_single_to_multi_bucket(allowed)

        super()._set_auth_data(
            account_id,
            auth_token,
            api_url,
            download_url,
            recommended_part_size,
            absolute_minimum_part_size,
            application_key,
            realm,
            s3_api_url,
            new_allowed,
            application_key_id,
        )


class AbstractAccountInfo(_OldAllowedMixin, v3.AbstractAccountInfo):
    def list_bucket_names_ids(self):
        return []  # Removed @abstractmethod decorator


class UrlPoolAccountInfo(_OldAllowedMixin, v3.UrlPoolAccountInfo):
    pass


class InMemoryAccountInfo(_OldAllowedMixin, v3.InMemoryAccountInfo):
    pass


class SqliteAccountInfo(_OldAllowedMixin, v3.SqliteAccountInfo):
    pass


class StubAccountInfo(_OldAllowedMixin, v3.StubAccountInfo):
    pass
