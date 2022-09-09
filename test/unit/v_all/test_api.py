######################################################################
#
# File: test/unit/v_all/test_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import B2Api, BucketTrackingMixin
from apiver_deps import B2HttpApiConfig
from apiver_deps import Bucket
from apiver_deps import InMemoryCache
from apiver_deps import EncryptionMode
from apiver_deps import EncryptionSetting
from apiver_deps import InMemoryAccountInfo
from apiver_deps import RawSimulator
from apiver_deps_exception import BucketIdNotFound
from ..test_base import TestBase


class TestApiBase(TestBase):
    B2_API_CLASS = B2Api

    def setUp(self):
        self.account_info = InMemoryAccountInfo()
        self.cache = InMemoryCache()
        self.api = self.B2_API_CLASS(
            self.account_info, self.cache, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.api.session.raw_api
        (self.application_key_id, self.master_key) = self.raw_api.create_account()

    def _authorize_account(self):
        self.api.authorize_account('production', self.application_key_id, self.master_key)


class TestApi(TestApiBase):
    @pytest.mark.apiver(to_ver=1)
    def test_get_bucket_by_id_up_to_v1(self):
        bucket = self.api.get_bucket_by_id("this id doesn't even exist")
        assert bucket.id_ == "this id doesn't even exist"
        for att_name, att_value in [
            ('name', None),
            ('type_', None),
            ('bucket_info', {}),
            ('cors_rules', []),
            ('lifecycle_rules', []),
            ('revision', None),
            ('bucket_dict', {}),
            ('options_set', set()),
            ('default_server_side_encryption', EncryptionSetting(EncryptionMode.UNKNOWN)),
        ]:
            with self.subTest(att_name=att_name):
                assert getattr(bucket, att_name) == att_value, att_name

    @pytest.mark.apiver(from_ver=2)
    def test_get_bucket_by_id_v2(self):
        self._authorize_account()
        with pytest.raises(BucketIdNotFound):
            self.api.get_bucket_by_id("this id doesn't even exist")
        created_bucket = self.api.create_bucket('bucket1', 'allPrivate')
        read_bucket = self.api.get_bucket_by_id(created_bucket.id_)
        assert created_bucket.id_ == read_bucket.id_
        self.cache.save_bucket(Bucket(api=self.api, name='bucket_name', id_='bucket_id'))
        read_bucket = self.api.get_bucket_by_id('bucket_id')
        assert read_bucket.name == 'bucket_name'

    def test_get_download_url_for_file_name(self):
        self._authorize_account()

        download_url = self.api.get_download_url_for_file_name('bucket1', 'some-file.txt')

        assert download_url == 'http://download.example.com/file/bucket1/some-file.txt'

    def test_get_download_url_for_fileid(self):
        self._authorize_account()

        download_url = self.api.get_download_url_for_fileid('file-id')

        assert download_url == 'http://download.example.com/b2api/v2/b2_download_file_by_id?fileId=file-id'


class TestBucketTrackingMixin(TestApiBase):
    class BucketTrackingApi(BucketTrackingMixin, B2Api):
        pass

    B2_API_CLASS = BucketTrackingApi

    def test_bucket_tracking(self):
        self._authorize_account()

        bucket_1, bucket_2, bucket_3 = [
            self.api.create_bucket(f'bucket-{i + 1}', 'allPrivate') for i in range(3)
        ]

        self.api.delete_bucket(bucket_2)
        self.api.delete_bucket(bucket_3)

        bucket_4 = self.api.create_bucket('bucket-4', 'allPrivate')

        assert {bucket.id_ for bucket in self.api.buckets} == {bucket_1.id_, bucket_4.id_}
