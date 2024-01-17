######################################################################
#
# File: test/unit/api/test_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import time
from contextlib import suppress
from unittest import mock

import apiver_deps
import pytest
from apiver_deps import (
    NO_RETENTION_FILE_SETTING,
    ApplicationKey,
    B2Api,
    B2Http,
    B2HttpApiConfig,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FileIdAndName,
    FileRetentionSetting,
    FullApplicationKey,
    InMemoryAccountInfo,
    InMemoryCache,
    LegalHold,
    RawSimulator,
    RetentionMode,
)
from apiver_deps_exception import AccessDenied, FileNotPresent, InvalidArgument, RestrictedBucket

from ..test_base import create_key

if apiver_deps.V <= 1:
    from apiver_deps import FileVersionInfo as VFileVersion
else:
    from apiver_deps import FileVersion as VFileVersion


class TestApi:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.account_info = InMemoryAccountInfo()
        self.cache = InMemoryCache()
        self.api = B2Api(
            self.account_info, self.cache, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.api.session.raw_api
        (self.application_key_id, self.master_key) = self.raw_api.create_account()

    @pytest.mark.apiver(from_ver=3)
    def test_authorize_signature_v3_default_realm(self):
        self.api.authorize_account(
            self.application_key_id,
            self.master_key,
        )

    @pytest.mark.apiver(from_ver=3)
    def test_authorize_signature_v3_explicit_realm(self):
        self.api.authorize_account(
            self.application_key_id,
            self.master_key,
            'production',
        )

    @pytest.mark.apiver(to_ver=2)
    def test_authorize_signature_up_to_ver_2(self):
        self.api.authorize_account(
            'production',
            self.application_key_id,
            self.master_key,
        )

    def test_get_file_info(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        created_file = bucket.upload_bytes(
            b'hello world', 'file', cache_control="private, max-age=3600"
        )

        result = self.api.get_file_info(created_file.id_)

        if apiver_deps.V <= 1:
            self.maxDiff = None
            with suppress(KeyError):
                del result['replicationStatus']
            assert result == {
                'accountId': 'account-0',
                'action': 'upload',
                'bucketId': 'bucket_0',
                'contentLength': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file',
                'fileRetention': {
                    'isClientAuthorizedToRead': True,
                    'value': {
                        'mode': None
                    }
                },
                'legalHold': {
                    'isClientAuthorizedToRead': True,
                    'value': None
                },
                'serverSideEncryption': {
                    'mode': 'none'
                },
                'uploadTimestamp': 5000,
            }
        else:
            assert isinstance(result, VFileVersion)
            assert result == created_file

    def test_get_file_info_by_name(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        bucket.upload_bytes(
            b'hello world',
            'file',
        )
        result = self.api.get_file_info_by_name('bucket1', 'file')

        expected_result = {
            'fileId': '9999',
            'fileName': 'file',
            'fileInfo': {},
            'serverSideEncryption': {
                'mode': 'none'
            },
            'legalHold': None,
            'fileRetention': {
                'mode': None,
                'retainUntilTimestamp': None
            },
            'size': 11,
            'uploadTimestamp': 5000,
            'contentType': 'b2/x-auto',
            'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
            'replicationStatus': None,
        }

        if apiver_deps.V <= 1:
            expected_result.update({
                'accountId': None,
                'action': 'upload',
                'bucketId': None,
            })

        assert result.as_dict() == expected_result

    def test_get_hidden_file_info_by_name(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        bucket.upload_bytes(
            b'hello world',
            'hidden-file.txt',
        )
        bucket.hide_file('hidden-file.txt')
        result = self.api.get_file_info_by_name('bucket1', 'hidden-file.txt')

        expected_result = {
            'fileId': '9998',
            'fileName': 'hidden-file.txt',
            'fileInfo': {},
            'serverSideEncryption': {
                'mode': 'none'
            },
            'legalHold': None,
            'fileRetention': {
                'mode': None,
                'retainUntilTimestamp': None
            },
            'size': 0,
            'uploadTimestamp': 5001,
            'contentSha1': 'none',
            'replicationStatus': None,
        }

        if apiver_deps.V <= 1:
            expected_result.update({
                'accountId': None,
                'action': 'upload',
                'bucketId': None,
            })

        assert result.as_dict() == expected_result

    def test_get_non_existent_file_info_by_name(self):
        self._authorize_account()
        _ = self.api.create_bucket('bucket1', 'allPrivate')
        with pytest.raises(FileNotPresent):
            _ = self.api.get_file_info_by_name('bucket1', 'file-does-not-exist.txt')

    def test_get_file_info_by_name_with_properties(self):
        encr_setting = EncryptionSetting(
            EncryptionMode.SSE_C, EncryptionAlgorithm.AES256, EncryptionKey(b'secret', None)
        )
        lh_setting = LegalHold.ON
        retention_setting = FileRetentionSetting(RetentionMode.COMPLIANCE, 100)

        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate', is_file_lock_enabled=True)
        _ = bucket.upload_bytes(
            b'hello world',
            'file',
            encryption=encr_setting,
            legal_hold=lh_setting,
            file_retention=retention_setting
        )

        result = self.api.get_file_info_by_name('bucket1', 'file')

        assert encr_setting.mode == result.server_side_encryption.mode
        assert encr_setting.algorithm == result.server_side_encryption.algorithm
        assert lh_setting == result.legal_hold
        assert retention_setting == result.file_retention

    @pytest.mark.parametrize(
        'expected_delete_bucket_output',
        [
            pytest.param(None, marks=pytest.mark.apiver(from_ver=1)),
            pytest.param(
                {
                    'accountId': 'account-0',
                    'bucketName': 'bucket2',
                    'bucketId': 'bucket_1',
                    'bucketType': 'allPrivate',
                    'bucketInfo': {},
                    'corsRules': [],
                    'lifecycleRules': [],
                    'options': set(),
                    'revision': 1,
                    'defaultServerSideEncryption':
                        {
                            'isClientAuthorizedToRead': True,
                            'value': {
                                'mode': 'none'
                            },
                        },
                    'fileLockConfiguration':
                        {
                            'isClientAuthorizedToRead': True,
                            'value':
                                {
                                    'defaultRetention': {
                                        'mode': None,
                                        'period': None
                                    },
                                    'isFileLockEnabled': None
                                }
                        },
                },
                marks=pytest.mark.apiver(to_ver=0)
            ),
        ],
    )
    def test_list_buckets(self, expected_delete_bucket_output):
        self._authorize_account()
        self.api.create_bucket('bucket1', 'allPrivate')
        bucket2 = self.api.create_bucket('bucket2', 'allPrivate')
        delete_output = self.api.delete_bucket(bucket2)
        if expected_delete_bucket_output is not None:
            with suppress(KeyError):
                del delete_output['replicationConfiguration']
        assert delete_output == expected_delete_bucket_output
        self.api.create_bucket('bucket3', 'allPrivate')
        assert [b.name for b in self.api.list_buckets()] == ['bucket1', 'bucket3']

    def test_list_buckets_with_name(self):
        self._authorize_account()
        self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        assert [b.name for b in self.api.list_buckets(bucket_name='bucket1')] == ['bucket1']

    @pytest.mark.apiver(from_ver=3)
    def test_list_buckets_from_cache(self):
        bucket = type("bucket", (), {"name": "bucket", "id_": "ID-0"})
        self._authorize_account()
        self.cache.set_bucket_name_cache([bucket])

        def list_buckets(*args, **kwargs):
            buckets = self.api.list_buckets(*args, **kwargs)
            return [(b.name, b.id_) for b in buckets]

        assert list_buckets(use_cache=True) == [('bucket', 'ID-0')]
        assert list_buckets(bucket_name="bucket", use_cache=True) == [('bucket', 'ID-0')]
        assert list_buckets(bucket_name="bucket2", use_cache=True) == []
        assert list_buckets(bucket_id="ID-0", use_cache=True) == [('bucket', 'ID-0')]
        assert list_buckets(bucket_id="ID-2", use_cache=True) == []
        assert self.api.list_buckets() == []

    def test_buckets_with_encryption(self):
        self._authorize_account()
        sse_b2_aes = EncryptionSetting(
            mode=EncryptionMode.SSE_B2,
            algorithm=EncryptionAlgorithm.AES256,
        )
        no_encryption = EncryptionSetting(mode=EncryptionMode.NONE,)
        unknown_encryption = EncryptionSetting(mode=EncryptionMode.UNKNOWN,)

        b1 = self.api.create_bucket(
            'bucket1',
            'allPrivate',
            default_server_side_encryption=sse_b2_aes,
        )
        self._verify_if_bucket_is_encrypted(b1, should_be_encrypted=True)

        b2 = self.api.create_bucket('bucket2', 'allPrivate')
        self._verify_if_bucket_is_encrypted(b2, should_be_encrypted=False)

        # uses list_buckets
        self._check_if_bucket_is_encrypted('bucket1', should_be_encrypted=True)
        self._check_if_bucket_is_encrypted('bucket2', should_be_encrypted=False)

        # update to set encryption on b2
        b2.update(default_server_side_encryption=sse_b2_aes)
        self._check_if_bucket_is_encrypted('bucket1', should_be_encrypted=True)
        self._check_if_bucket_is_encrypted('bucket2', should_be_encrypted=True)

        # update to unset encryption again
        b2.update(default_server_side_encryption=no_encryption)
        self._check_if_bucket_is_encrypted('bucket1', should_be_encrypted=True)
        self._check_if_bucket_is_encrypted('bucket2', should_be_encrypted=False)

        # now check it with no readBucketEncryption permission to see that it's unknown
        key = create_key(self.api, ['listBuckets'], 'key1')
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )
        buckets = {
            b.name: b
            for b in self.api.list_buckets()  # scan again with new key
        }

        assert buckets['bucket1'].default_server_side_encryption == unknown_encryption

        assert buckets['bucket2'].default_server_side_encryption == unknown_encryption

    def _check_if_bucket_is_encrypted(self, bucket_name, should_be_encrypted):
        buckets = {b.name: b for b in self.api.list_buckets()}
        bucket = buckets[bucket_name]
        return self._verify_if_bucket_is_encrypted(bucket, should_be_encrypted)

    def _verify_if_bucket_is_encrypted(self, bucket, should_be_encrypted):
        sse_b2_aes = EncryptionSetting(
            mode=EncryptionMode.SSE_B2,
            algorithm=EncryptionAlgorithm.AES256,
        )
        no_encryption = EncryptionSetting(mode=EncryptionMode.NONE,)
        if not should_be_encrypted:
            assert bucket.default_server_side_encryption == no_encryption
        else:
            assert bucket.default_server_side_encryption == sse_b2_aes
            assert bucket.default_server_side_encryption.mode == EncryptionMode.SSE_B2
            assert bucket.default_server_side_encryption.algorithm == EncryptionAlgorithm.AES256

    def test_list_buckets_with_id(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        assert [b.name for b in self.api.list_buckets(bucket_id=bucket.id_)] == ['bucket1']

    def test_reauthorize_with_app_key(self):
        # authorize and create a key
        self._authorize_account()
        key = create_key(self.api, ['listBuckets'], 'key1')

        # authorize with the key
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )

        # expire the auth token we just got
        self.raw_api.expire_auth_token(self.account_info.get_account_auth_token())

        # listing buckets should work, after it re-authorizes
        self.api.list_buckets()

    def test_list_buckets_with_restriction(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = create_key(self.api, ['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )
        assert [b.name for b in self.api.list_buckets(bucket_name=bucket1.name)] == ['bucket1']

    def test_get_bucket_by_name_with_bucket_restriction(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        key = create_key(self.api, ['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )
        assert self.api.get_bucket_by_name('bucket1').id_ == bucket1.id_

    def test_list_buckets_with_restriction_and_wrong_name(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        bucket2 = self.api.create_bucket('bucket2', 'allPrivate')
        key = create_key(self.api, ['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )
        with pytest.raises(RestrictedBucket) as excinfo:
            self.api.list_buckets(bucket_name=bucket2.name)
        assert str(excinfo.value) == 'Application key is restricted to bucket: bucket1'

    def test_list_buckets_with_restriction_and_no_name(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = create_key(self.api, ['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )
        with pytest.raises(RestrictedBucket) as excinfo:
            self.api.list_buckets()
        assert str(excinfo.value) == 'Application key is restricted to bucket: bucket1'

    def test_list_buckets_with_restriction_and_wrong_id(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = create_key(self.api, ['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )
        with pytest.raises(RestrictedBucket) as excinfo:
            self.api.list_buckets(bucket_id='not the one bound to the key')
        assert str(excinfo.value) == f'Application key is restricted to bucket: {bucket1.id_}'

    def _authorize_account(self):
        self.api.authorize_account(
            application_key_id=self.application_key_id,
            application_key=self.master_key,
            realm='production',
        )

    def test_update_file_retention(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate', is_file_lock_enabled=True)
        created_file = bucket.upload_bytes(b'hello world', 'file')
        assert created_file.file_retention == NO_RETENTION_FILE_SETTING
        new_retention = FileRetentionSetting(RetentionMode.COMPLIANCE, 100)
        read_file_retention = self.api.update_file_retention(
            created_file.id_, created_file.file_name, new_retention
        )
        assert new_retention == read_file_retention
        if apiver_deps.V <= 1:
            file_version = bucket.get_file_info_by_id(created_file.id_)
        else:
            file_version = self.api.get_file_info(created_file.id_)
        assert new_retention == file_version.file_retention

    def test_update_legal_hold(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate', is_file_lock_enabled=True)
        created_file = bucket.upload_bytes(b'hello world', 'file')
        assert created_file.legal_hold == LegalHold.UNSET
        new_legal_hold = LegalHold.ON
        read_legal_hold = self.api.update_file_legal_hold(
            created_file.id_, created_file.file_name, new_legal_hold
        )
        assert new_legal_hold == read_legal_hold
        if apiver_deps.V <= 1:
            file_version = bucket.get_file_info_by_id(created_file.id_)
        else:
            file_version = self.api.get_file_info(created_file.id_)
        assert new_legal_hold == file_version.legal_hold

    @pytest.mark.apiver(from_ver=2)
    def test_cancel_large_file_v2(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        unfinished_large_file = self.api.services.large_file.start_large_file(
            bucket.id_, 'a_large_file'
        )
        cancel_result = self.api.cancel_large_file(unfinished_large_file.file_id)
        assert cancel_result == FileIdAndName('9999', 'a_large_file')

    @pytest.mark.apiver(to_ver=1)
    def test_cancel_large_file_v1(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        unfinished_large_file = self.api.services.large_file.start_large_file(
            bucket.id_, 'a_large_file'
        )
        cancel_result = self.api.cancel_large_file(unfinished_large_file.file_id)
        assert cancel_result == VFileVersion(
            id_='9999',
            file_name='a_large_file',
            size=0,
            content_type='unknown',
            content_sha1='none',
            file_info={},
            upload_timestamp=0,
            action='cancel',
            api=self.api,
        )

    @pytest.mark.apiver(to_ver=1)
    def test_provide_raw_api_v1(self):
        from apiver_deps import B2RawApi  # test for legacy name
        old_style_api = B2Api(raw_api=B2RawApi(B2Http(user_agent_append='test append')))
        new_style_api = B2Api(api_config=B2HttpApiConfig(user_agent_append='test append'))
        assert old_style_api.session.raw_api.b2_http.user_agent == new_style_api.session.raw_api.b2_http.user_agent
        with pytest.raises(InvalidArgument):
            B2Api(
                raw_api=B2RawApi(B2Http(user_agent_append='test append')),
                api_config=B2HttpApiConfig(user_agent_append='test append'),
            )

    @pytest.mark.apiver(to_ver=1)
    def test_create_and_delete_key_v1(self):
        self._authorize_account()
        create_result = self.api.create_key(['readFiles'], 'testkey')
        assert create_result == {
            'accountId': self.account_info.get_account_id(),
            'applicationKey': 'appKey0',
            'applicationKeyId': 'appKeyId0',
            'capabilities': ['readFiles'],
            'keyName': 'testkey',
        }

        delete_result = self.api.delete_key(create_result['applicationKeyId'])
        self.assertDeleteAndCreateResult(create_result, delete_result)

        create_result = self.api.create_key(['readFiles'], 'testkey')
        delete_result = self.api.delete_key_by_id(create_result['applicationKeyId'])
        self.assertDeleteAndCreateResult(create_result, delete_result.as_dict())

    @pytest.mark.apiver(from_ver=2)
    def test_create_and_delete_key_v2(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket', 'allPrivate')
        now = time.time()
        create_result = self.api.create_key(
            ['readFiles'],
            'testkey',
            valid_duration_seconds=100,
            bucket_id=bucket.id_,
            name_prefix='name',
        )
        assert isinstance(create_result, FullApplicationKey)
        assert create_result.key_name == 'testkey'
        assert create_result.capabilities == ['readFiles']
        assert create_result.account_id == self.account_info.get_account_id()
        assert (now + 100 -
                10) * 1000 < create_result.expiration_timestamp_millis < (now + 100 + 10) * 1000
        assert create_result.bucket_id == bucket.id_
        assert create_result.name_prefix == 'name'
        # assert create_result.options == ...  TODO

        delete_result = self.api.delete_key(create_result)
        self.assertDeleteAndCreateResult(create_result, delete_result)

        create_result = self.api.create_key(
            ['readFiles'],
            'testkey',
            valid_duration_seconds=100,
            bucket_id=bucket.id_,
            name_prefix='name',
        )
        delete_result = self.api.delete_key_by_id(create_result.id_)
        self.assertDeleteAndCreateResult(create_result, delete_result)

    def assertDeleteAndCreateResult(self, create_result, delete_result):
        if apiver_deps.V <= 1:
            create_result.pop('applicationKey')
            assert delete_result == create_result
        else:
            assert isinstance(delete_result, ApplicationKey)
            assert delete_result.key_name == create_result.key_name
            assert delete_result.capabilities == create_result.capabilities
            assert delete_result.account_id == create_result.account_id
            assert delete_result.expiration_timestamp_millis == create_result.expiration_timestamp_millis
            assert delete_result.bucket_id == create_result.bucket_id
            assert delete_result.name_prefix == create_result.name_prefix

    @pytest.mark.apiver(to_ver=1)
    def test_list_keys_v1(self):
        self._authorize_account()
        for i in range(20):
            self.api.create_key(['readFiles'], f'testkey{i}')
        with mock.patch.object(self.api, 'DEFAULT_LIST_KEY_COUNT', 10):
            response = self.api.list_keys()
        assert response['nextApplicationKeyId'] == 'appKeyId18'
        assert response['keys'] == [
            {
                'accountId': 'account-0',
                'applicationKeyId': f'appKeyId{ind}',
                'bucketId': None,
                'capabilities': ['readFiles'],
                'expirationTimestamp': None,
                'keyName': f'testkey{ind}',
                'namePrefix': None,
            } for ind in [
                0,
                1,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
            ]
        ]

    @pytest.mark.apiver(from_ver=2)
    def test_list_keys_v2(self):
        self._authorize_account()
        for i in range(20):
            self.api.create_key(['readFiles'], f'testkey{i}')
        with mock.patch.object(self.api, 'DEFAULT_LIST_KEY_COUNT', 10):
            keys = list(self.api.list_keys())
        assert [key.id_ for key in keys] == [
            'appKeyId0',
            'appKeyId1',
            'appKeyId10',
            'appKeyId11',
            'appKeyId12',
            'appKeyId13',
            'appKeyId14',
            'appKeyId15',
            'appKeyId16',
            'appKeyId17',
            'appKeyId18',
            'appKeyId19',
            'appKeyId2',
            'appKeyId3',
            'appKeyId4',
            'appKeyId5',
            'appKeyId6',
            'appKeyId7',
            'appKeyId8',
            'appKeyId9',
        ]
        assert isinstance(keys[0], ApplicationKey)

    def test_get_key(self):
        self._authorize_account()
        key = self.api.create_key(['readFiles'], 'testkey')

        if apiver_deps.V <= 1:
            key_id = key['applicationKeyId']
        else:
            key_id = key.id_

        assert self.api.get_key(key_id) is not None

        if apiver_deps.V <= 1:
            self.api.delete_key(key_id)
        else:
            self.api.delete_key(key)

        assert self.api.get_key(key_id) is None
        assert self.api.get_key('non-existent') is None

    def test_delete_file_version_bypass_governance(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        created_file = bucket.upload_bytes(
            b'hello world',
            'file',
            file_retention=FileRetentionSetting(RetentionMode.GOVERNANCE,
                                                int(time.time()) + 100),
        )

        with pytest.raises(AccessDenied):
            self.api.delete_file_version(created_file.id_, 'file')

        self.api.delete_file_version(created_file.id_, 'file', bypass_governance=True)
        with pytest.raises(FileNotPresent):
            bucket.get_file_info_by_name(created_file.file_name)
