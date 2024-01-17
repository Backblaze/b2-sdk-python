######################################################################
#
# File: test/unit/v_all/test_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from apiver_deps import (
    B2Api,
    B2HttpApiConfig,
    Bucket,
    EncryptionMode,
    EncryptionSetting,
    InMemoryAccountInfo,
    InMemoryCache,
    RawSimulator,
)
from apiver_deps_exception import BucketIdNotFound

from ..test_base import TestBase


class DummyA:
    def __init__(self, *args, **kwargs):
        pass


class DummyB:
    def __init__(self, *args, **kwargs):
        pass


class TestServices:
    @pytest.mark.apiver(from_ver=2)
    @pytest.mark.parametrize(
        ('kwargs', '_raw_api_class'),
        [
            [
                {
                    'max_upload_workers': 1,
                    'max_copy_workers': 2,
                    'max_download_workers': 3,
                    'save_to_buffer_size': 4,
                    'check_download_hash': False,
                    'max_download_streams_per_file': 5,
                },
                DummyA,
            ],
            [
                {
                    'max_upload_workers': 2,
                    'max_copy_workers': 3,
                    'max_download_workers': 4,
                    'save_to_buffer_size': 5,
                    'check_download_hash': True,
                    'max_download_streams_per_file': 6,
                },
                DummyB,
            ],
        ],
    )  # yapf: disable
    def test_api_initialization(self, kwargs, _raw_api_class):
        self.account_info = InMemoryAccountInfo()
        self.cache = InMemoryCache()

        api_config = B2HttpApiConfig(_raw_api_class=_raw_api_class)

        self.api = B2Api(
            self.account_info,
            self.cache,
            api_config=api_config,

            **kwargs
        )  # yapf: disable

        assert self.api.account_info is self.account_info
        assert self.api.api_config is api_config
        assert self.api.cache is self.cache

        assert self.api.session.account_info is self.account_info
        assert self.api.session.cache is self.cache
        assert isinstance(self.api.session.raw_api, _raw_api_class)

        assert isinstance(self.api.file_version_factory, B2Api.FILE_VERSION_FACTORY_CLASS)
        assert isinstance(
            self.api.download_version_factory,
            B2Api.DOWNLOAD_VERSION_FACTORY_CLASS,
        )

        services = self.api.services
        assert isinstance(services, B2Api.SERVICES_CLASS)

        # max copy/upload/download workers could only be verified with mocking

        download_manager = services.download_manager
        assert isinstance(download_manager, services.DOWNLOAD_MANAGER_CLASS)

        assert download_manager.write_buffer_size == kwargs['save_to_buffer_size']
        assert download_manager.check_hash == kwargs['check_download_hash']
        assert download_manager.strategies[0].max_streams == kwargs['max_download_streams_per_file']


class TestApi(TestBase):
    def setUp(self):
        self.account_info = InMemoryAccountInfo()
        self.cache = InMemoryCache()
        self.api = B2Api(
            self.account_info, self.cache, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.api.session.raw_api
        (self.application_key_id, self.master_key) = self.raw_api.create_account()

    def _authorize_account(self):
        self.api.authorize_account(
            realm='production',
            application_key_id=self.application_key_id,
            application_key=self.master_key,
        )

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
