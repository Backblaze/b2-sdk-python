######################################################################
#
# File: test/unit/v_all/test_replication.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import pytest

from apiver_deps import B2Api
from apiver_deps import B2HttpApiConfig
from apiver_deps import Bucket
from apiver_deps import InMemoryCache
from apiver_deps import InMemoryAccountInfo
from apiver_deps import RawSimulator
from ..test_base import TestBase

from b2sdk.replication.setup import ReplicationSetupHelper

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
        self.api.authorize_account('production', self.application_key_id, self.master_key)

    @pytest.mark.apiver(from_ver=2)
    def test_get_bucket_by_id_v2(self):
        self._authorize_account()
        #with pytest.raises(BucketIdNotFound):
        #    self.api.get_bucket_by_id("this id doesn't even exist")
        source_bucket = self.api.create_bucket('bucket1', 'allPrivate')
        destination_bucket = self.api.create_bucket('bucket2', 'allPrivate')
        #read_bucket = self.api.get_bucket_by_id(source_bucket.id_)
        #assert source_bucket.id_ == read_bucket.id_
        #self.cache.save_bucket(Bucket(api=self.api, name='bucket_name', id_='bucket_id'))
        #read_bucket = self.api.get_bucket_by_id('bucket_id')
        #assert read_bucket.name == 'bucket_name'
        logger.info('preparations complete, starting the test')
        rsh = ReplicationSetupHelper(
            source_b2api=self.api,
            destination_b2api=self.api,
        )
        source_bucket, destination_bucket = rsh.setup_both(
            source_bucket_path="bucket1",
            destination_bucket=destination_bucket,
            name='aa',
        )
        print(source_bucket.replication)
        # ReplicationConfiguration(as_replication_source=ReplicationSourceConfiguration(rules=[ReplicationRule(destination_bucket_id='bucket_1', name='aa', file_name_prefix='', is_enabled=True, priority=128)], source_application_key_id=<b2sdk.application_key.FullApplicationKey object at 0x7f273162cbe0>), as_replication_destination=ReplicationDestinationConfiguration(source_to_destination_key_mapping={}))
        print('---')
        print(destination_bucket.replication)
        # ReplicationConfiguration(as_replication_source=ReplicationSourceConfiguration(rules=[], source_application_key_id=None), as_replication_destination=ReplicationDestinationConfiguration(source_to_destination_key_mapping={<b2sdk.application_key.FullApplicationKey object at 0x7f273162cbe0>: <b2sdk.application_key.FullApplicationKey object at 0x7f273162cac8>}))
