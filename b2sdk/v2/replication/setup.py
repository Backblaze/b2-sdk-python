######################################################################
#
# File: b2sdk/v2/replication/setup.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal.replication.setup import *  # noqa

from b2sdk import v3 as v3

from ..bucket import Bucket

from ..application_key import ApplicationKey  # type: ignore


class ReplicationSetupHelper(v3.ReplicationSetupHelper):  # type: ignore
    def setup_source(
        self,
        source_bucket: Bucket,
        source_key: ApplicationKey,
        destination_bucket: Bucket,
        prefix: str | None = None,
        name: str | None = None,
        priority: int = None,
        include_existing_files: bool = False,
    ) -> Bucket:
        return super().setup_source(
            source_bucket=source_bucket,
            source_key=source_key,
            destination_bucket=destination_bucket,
            prefix=prefix,
            name=name,
            priority=priority,
            include_existing_files=include_existing_files,
        )

    @classmethod
    def _get_source_key(
        cls,
        source_bucket: Bucket,
        prefix: str,
        current_replication_configuration: ReplicationConfiguration,
    ) -> ApplicationKey:
        return super()._get_source_key(
            source_bucket=source_bucket,
            prefix=prefix,
            current_replication_configuration=current_replication_configuration,
        )

    @classmethod
    def _should_make_new_source_key(
        cls,
        current_replication_configuration: ReplicationConfiguration,
        current_source_key: ApplicationKey | None,
    ) -> bool:
        return super()._should_make_new_source_key(
            current_replication_configuration=current_replication_configuration,
            current_source_key=current_source_key,
        )

    @classmethod
    def _create_source_key(
        cls,
        name: str,
        bucket: Bucket,
        prefix: str | None = None,
    ) -> ApplicationKey:
        return super()._create_source_key(
            name=name,
            bucket=bucket,
            prefix=prefix,
        )

    @classmethod
    def _create_destination_key(
        cls,
        name: str,
        bucket: Bucket,
        prefix: str | None = None,
    ) -> ApplicationKey:
        return super()._create_destination_key(
            name=name,
            bucket=bucket,
            prefix=prefix,
        )

    @classmethod
    def _create_key(
        cls,
        name: str,
        bucket: Bucket,
        prefix: str | None = None,
        capabilities=tuple(),
    ) -> ApplicationKey:
        api: B2Api = bucket.api
        return api.create_key(
            capabilities=capabilities,
            key_name=name,
            bucket_id=bucket.id_,
            name_prefix=prefix,
        )
