######################################################################
#
# File: test/unit/v1/test_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..test_base import TestBase

from .deps import B2Api
from .deps import DummyCache
from .deps import EncryptionAlgorithm
from .deps import EncryptionMode
from .deps import EncryptionSetting
from .deps import FileRetentionSetting
from .deps import InMemoryAccountInfo
from .deps import LegalHold
from .deps import RawSimulator
from .deps import RetentionMode
from .deps import NO_RETENTION_FILE_SETTING
from .deps_exception import RestrictedBucket


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
        bucket2 = self.api.create_bucket('bucket2', 'allPrivate')
        delete_output = self.api.delete_bucket(bucket2)
        assert delete_output is None, delete_output
        self.api.create_bucket('bucket3', 'allPrivate')
        self.assertEqual(
            ['bucket1', 'bucket3'],
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
        key = self.api.create_key(['listBuckets'], 'key1')
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        buckets = {
            b.name: b
            for b in self.api.list_buckets()  # scan again with new key
        }

        self.assertEqual(
            buckets['bucket1'].default_server_side_encryption,
            unknown_encryption,
        )

        self.assertEqual(
            buckets['bucket2'].default_server_side_encryption,
            unknown_encryption,
        )

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
            self.assertEqual(
                bucket.default_server_side_encryption,
                no_encryption,
            )
        else:
            self.assertEqual(
                bucket.default_server_side_encryption,
                sse_b2_aes,
            )
            self.assertEqual(
                bucket.default_server_side_encryption.mode,
                EncryptionMode.SSE_B2,
            )
            self.assertEqual(
                bucket.default_server_side_encryption.algorithm,
                EncryptionAlgorithm.AES256,
            )

    def test_list_buckets_with_id(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        self.assertEqual(
            ['bucket1'],
            [b.name for b in self.api.list_buckets(bucket_id=bucket.id_)],
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
        with self.assertRaises(
            RestrictedBucket, 'Application key is restricted to bucket: bucket1'
        ):
            self.api.list_buckets(bucket_name=bucket2.name)

    def test_list_buckets_with_restriction_and_no_name(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = self.api.create_key(['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        with self.assertRaises(
            RestrictedBucket, 'Application key is restricted to bucket: bucket1'
        ):
            self.api.list_buckets()

    def test_list_buckets_with_restriction_and_wrong_id(self):
        self._authorize_account()
        bucket1 = self.api.create_bucket('bucket1', 'allPrivate')
        self.api.create_bucket('bucket2', 'allPrivate')
        key = self.api.create_key(['listBuckets'], 'key1', bucket_id=bucket1.id_)
        self.api.authorize_account('production', key['applicationKeyId'], key['applicationKey'])
        with self.assertRaises(
            RestrictedBucket, 'Application key is restricted to bucket: %s' % (bucket1.id_,)
        ):
            self.api.list_buckets(bucket_id='not the one bound to the key')

    def _authorize_account(self):
        self.api.authorize_account('production', self.application_key_id, self.master_key)

    def test_update_file_retention(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate', is_file_lock_enabled=True)
        created_file = bucket.upload_bytes(b'hello world', 'file')
        self.assertEqual(created_file.file_retention, NO_RETENTION_FILE_SETTING)
        new_retention = FileRetentionSetting(RetentionMode.COMPLIANCE, 100)
        self.api.update_file_retention(created_file.id_, created_file.file_name, new_retention)
        file_version = bucket.get_file_info_by_id(created_file.id_)
        self.assertEqual(new_retention, file_version.file_retention)

    def test_update_legal_hold(self):
        self._authorize_account()
        bucket = self.api.create_bucket('bucket1', 'allPrivate', is_file_lock_enabled=True)
        created_file = bucket.upload_bytes(b'hello world', 'file')
        self.assertEqual(created_file.legal_hold, LegalHold.UNSET)
        new_legal_hold = LegalHold.ON
        self.api.update_file_legal_hold(created_file.id_, created_file.file_name, new_legal_hold)
        file_version = bucket.get_file_info_by_id(created_file.id_)
        self.assertEqual(new_legal_hold, file_version.legal_hold)
