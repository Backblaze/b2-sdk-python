######################################################################
#
# File: test/unit/v_all/test_replication.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging

import pytest
from apiver_deps import (
    B2Api,
    B2HttpApiConfig,
    InMemoryAccountInfo,
    InMemoryCache,
    RawSimulator,
    ReplicationConfiguration,
    ReplicationRule,
    ReplicationSetupHelper,
)

from ..test_base import TestBase

logger = logging.getLogger(__name__)


class TestReplication(TestBase):
    def setUp(self):
        self.account_info = InMemoryAccountInfo()
        self.cache = InMemoryCache()
        self.api = B2Api(
            self.account_info, self.cache, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.api.session.raw_api
        self.application_key_id, self.master_key = self.raw_api.create_account()

    def _authorize_account(self):
        self.api.authorize_account(
            realm='production',
            application_key_id=self.application_key_id,
            application_key=self.master_key,
        )

    @pytest.mark.apiver(from_ver=2)
    def test_setup_both(self):
        self._authorize_account()
        source_bucket = self.api.create_bucket('bucket1', 'allPrivate')
        destination_bucket = self.api.create_bucket('bucket2', 'allPrivate')
        logger.info('preparations complete, starting the test')
        rsh = ReplicationSetupHelper()
        source_bucket, destination_bucket = rsh.setup_both(
            source_bucket=source_bucket,
            destination_bucket=destination_bucket,
            name='aa',
            prefix='ab',
        )

        from pprint import pprint
        pprint([k.as_dict() for k in self.api.list_keys()])

        keymap = {k.key_name: k for k in self.api.list_keys()}

        source_application_key = keymap['bucket1-replisrc']
        assert source_application_key
        assert set(source_application_key.capabilities) == set(
            ('readFiles', 'readFileLegalHolds', 'readFileRetentions')
        )
        assert not source_application_key.name_prefix
        assert source_application_key.expiration_timestamp_millis is None

        destination_application_key = keymap['bucket2-replidst']
        assert destination_application_key
        assert set(destination_application_key.capabilities) == set(
            ('writeFiles', 'writeFileLegalHolds', 'writeFileRetentions', 'deleteFiles')
        )
        assert not destination_application_key.name_prefix
        assert destination_application_key.expiration_timestamp_millis is None

        assert source_bucket.replication.rules == [
            ReplicationRule(
                destination_bucket_id='bucket_1',
                name='aa',
                file_name_prefix='ab',
                is_enabled=True,
                priority=128,
            )
        ]
        assert source_bucket.replication.source_key_id == source_application_key.id_
        assert source_bucket.replication.source_to_destination_key_mapping == {}

        print(destination_bucket.replication)
        assert destination_bucket.replication.rules == []
        assert destination_bucket.replication.source_key_id is None
        assert destination_bucket.replication.source_to_destination_key_mapping == {
            source_application_key.id_: destination_application_key.id_
        }

        old_source_application_key = source_application_key

        source_bucket, destination_bucket = rsh.setup_both(
            source_bucket=source_bucket,
            destination_bucket=destination_bucket,
            prefix='ad',
            include_existing_files=True,
        )

        keymap = {k.key_name: k for k in self.api.list_keys()}
        new_source_application_key = keymap['bucket1-replisrc']
        assert source_bucket.replication.rules == [
            ReplicationRule(
                destination_bucket_id='bucket_1',
                name='aa',
                file_name_prefix='ab',
                is_enabled=True,
                priority=128,
            ),
            ReplicationRule(
                destination_bucket_id='bucket_1',
                name='bucket2',
                file_name_prefix='ad',
                is_enabled=True,
                priority=133,
                include_existing_files=True,
            ),
        ]
        assert source_bucket.replication.source_key_id == old_source_application_key.id_

        assert destination_bucket.replication.source_to_destination_key_mapping == {
            new_source_application_key.id_: destination_application_key.id_
        }

    @pytest.mark.apiver(from_ver=2)
    def test_factory(self):
        replication = ReplicationConfiguration.from_dict({})
        assert replication == ReplicationConfiguration()
