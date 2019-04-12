######################################################################
#
# File: test/test_api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .test_base import TestBase
from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api
from b2sdk.cache import DummyCache
from b2sdk.exception import RestrictedBucket
from b2sdk.raw_simulator import RawSimulator


class TestApi(TestBase):
    def setUp(self):
        self.account_info = InMemoryAccountInfo()
        self.cache = DummyCache()
        self.raw_api = RawSimulator()
        self.api = B2Api(self.account_info, self.cache, self.raw_api)
        (self.application_key_id, self.master_key) = self.raw_api.create_account()

    def test_list_buckets(self):
        self._authorize_account()
        self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        self.assertEqual(
            ['bucket1', 'bucket2'],
            [b.name for b in self.api.list_buckets()],
        )

    def test_list_buckets_with_name(self):
        self._authorize_account()
        self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        self.assertEqual(
            ['bucket1'],
            [b.name for b in self.api.list_buckets(bucket_name='bucket1')],
        )

    def test_reauthorize_with_app_key(self):
        # authorize and create a key
        self._authorize_account()
        key = self.api.create_key(['listBuckets'], 'key1')

        # authorize with the key
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])

        # expire the auth token we just got
        self.raw_api.expire_auth_token(self.account_info.get_account_auth_token())

        # listing buckets should work, after it re-authorizes
        self.api.list_buckets()

    def test_list_buckets_with_restriction(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = self.api.create_key(['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        self.assertEqual(
            ['bucket1'],
            [b.name for b in self.api.list_buckets(bucket_name=bucket1.name)],
        )

    def test_get_bucket_by_name_with_bucket_restriction(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        key = self.api.create_key(['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        self.assertEqual(
            bucket1.id_,
            self.api.get_bucket_by_name('bucket1').id_,
        )

    def test_list_buckets_with_restriction_and_wrong_name(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        bucket2 = self.api.create_bucket('bucket2', 'allPrivate')
        key = self.api.create_key(['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        with self.assertRaises(RestrictedBucket):
            self.api.list_buckets(bucket_name=bucket2.name)

    def test_list_buckets_with_restriction_and_no_name(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = self.api.create_key(['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        with self.assertRaises(RestrictedBucket):
            self.api.list_buckets()

    def _authorize_account(self):
        self.api.authorize_account('production', self.application_key_id, self.master_key)
