######################################################################
#
# File: test/integration/test_encryption.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io

import pytest

from b2sdk.v3 import (
    SSE_B2_AES,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    UploadSourceBytes,
    WriteIntent,
)
from b2sdk.v3.exception import (
    WrongEncryptionModeForBucketDefault,
    WrongEncryptionSettingForFileWrite,
)

NO_ENCRYPTION = EncryptionSetting(mode=EncryptionMode.NONE)
UNKNOWN_ENCRYPTION = EncryptionSetting(mode=EncryptionMode.UNKNOWN)
SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'customer-key', key_id='customer-key-id'),
)
DEFAULT_ENCRYPTION = SSE_B2_AES
ENCRYPTION_OMITTED = object()
FILE_CONTENTS = b'hello world'
INVALID_BUCKET_ENCRYPTIONS = [
    pytest.param(NO_ENCRYPTION, id='no-encryption'),
    pytest.param(UNKNOWN_ENCRYPTION, id='unknown'),
    pytest.param(SSE_C_AES, id='sse-c'),
]
INVALID_FILE_ENCRYPTIONS = [
    pytest.param(NO_ENCRYPTION, id='no-encryption'),
    pytest.param(UNKNOWN_ENCRYPTION, id='unknown'),
]


def encryption_kwargs(encryption):
    if encryption is ENCRYPTION_OMITTED:
        return {}
    return {'encryption': encryption}


@pytest.fixture(scope='module')
def multipart_data(b2_api):
    part_size = b2_api.account_info.get_absolute_minimum_part_size() + 1
    return b'x' * (part_size * 2), part_size


class TestBucketDefaultEncryption:
    @pytest.mark.parametrize(
        'encryption',
        [
            pytest.param(ENCRYPTION_OMITTED, id='omitted'),
            pytest.param(SSE_B2_AES, id='explicit-sse-b2'),
        ],
    )
    def test_create_bucket_is_sse_b2(self, b2_api, bucket_manager, encryption):
        kwargs = (
            {}
            if encryption is ENCRYPTION_OMITTED
            else {'default_server_side_encryption': encryption}
        )
        bucket = b2_api.create_bucket(bucket_manager.new_bucket_name(), 'allPrivate', **kwargs)
        try:
            assert bucket.default_server_side_encryption == DEFAULT_ENCRYPTION
        finally:
            bucket_manager.clean_bucket(bucket)

    @pytest.mark.parametrize('encryption', INVALID_BUCKET_ENCRYPTIONS)
    def test_create_bucket_rejects_invalid_encryption(self, b2_api, bucket_manager, encryption):
        with pytest.raises(WrongEncryptionModeForBucketDefault):
            b2_api.create_bucket(
                bucket_manager.new_bucket_name(),
                'allPrivate',
                default_server_side_encryption=encryption,
            )

    @pytest.mark.parametrize('encryption', INVALID_BUCKET_ENCRYPTIONS)
    def test_update_bucket_rejects_invalid_encryption(self, bucket, encryption):
        with pytest.raises(WrongEncryptionModeForBucketDefault):
            bucket.update(default_server_side_encryption=encryption)


class FileCreationEncryptionTestBase:
    def _create_file(self, bucket, tmp_path, encryption):
        raise NotImplementedError

    def test_defaults_to_sse_b2(self, bucket, tmp_path):
        file_version = self._create_file(bucket, tmp_path, ENCRYPTION_OMITTED)

        assert file_version.server_side_encryption == DEFAULT_ENCRYPTION

    def test_accepts_explicit_sse_b2(self, bucket, tmp_path):
        file_version = self._create_file(bucket, tmp_path, SSE_B2_AES)

        assert file_version.server_side_encryption == SSE_B2_AES

    @pytest.mark.parametrize('encryption', INVALID_FILE_ENCRYPTIONS)
    def test_rejects_invalid_encryption(self, bucket, tmp_path, encryption):
        with pytest.raises(WrongEncryptionSettingForFileWrite):
            self._create_file(bucket, tmp_path, encryption)


class TestUploadBytesEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.upload_bytes(
            FILE_CONTENTS,
            'file',
            **encryption_kwargs(encryption),
        )


class TestUploadLocalFileEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        source = tmp_path / 'source'
        source.write_bytes(FILE_CONTENTS)
        return bucket.upload_local_file(
            source,
            'file',
            **encryption_kwargs(encryption),
        )


class TestUploadUnboundStreamEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.upload_unbound_stream(
            io.BytesIO(FILE_CONTENTS),
            'file',
            **encryption_kwargs(encryption),
        )


class TestUploadEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.upload(
            UploadSourceBytes(FILE_CONTENTS),
            'file',
            **encryption_kwargs(encryption),
        )


class TestCreateFileEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.create_file(
            [WriteIntent(UploadSourceBytes(FILE_CONTENTS))],
            'file',
            **encryption_kwargs(encryption),
        )


class TestCreateFileStreamEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.create_file_stream(
            iter([WriteIntent(UploadSourceBytes(FILE_CONTENTS))]),
            'file',
            **encryption_kwargs(encryption),
        )


class TestConcatenateEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.concatenate(
            [UploadSourceBytes(FILE_CONTENTS)],
            'file',
            **encryption_kwargs(encryption),
        )


class TestConcatenateStreamEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        return bucket.concatenate_stream(
            iter([UploadSourceBytes(FILE_CONTENTS)]),
            'file',
            **encryption_kwargs(encryption),
        )


class TestCopyEncryption(FileCreationEncryptionTestBase):
    def _create_file(self, bucket, tmp_path, encryption):
        source = bucket.upload_bytes(FILE_CONTENTS, 'source')
        kwargs = {} if encryption is ENCRYPTION_OMITTED else {'destination_encryption': encryption}
        return bucket.copy(
            source.id_,
            'file',
            **kwargs,
        )


class TestMultipartUploadEncryption(FileCreationEncryptionTestBase):
    @pytest.fixture(autouse=True)
    def _multipart_data(self, multipart_data):
        self.data, self.part_size = multipart_data

    def _create_file(self, bucket, tmp_path, encryption):
        file_version = bucket.upload_unbound_stream(
            io.BytesIO(self.data),
            'file',
            recommended_upload_part_size=self.part_size,
            **encryption_kwargs(encryption),
        )
        assert file_version._type() == 'large'
        return file_version


class TestMultipartCopyEncryption(FileCreationEncryptionTestBase):
    @pytest.fixture(autouse=True)
    def _multipart_data(self, multipart_data):
        self.data, self.part_size = multipart_data

    def _create_file(self, bucket, tmp_path, encryption):
        source = bucket.upload_bytes(self.data, 'source')
        kwargs = {} if encryption is ENCRYPTION_OMITTED else {'destination_encryption': encryption}
        file_version = bucket.copy(
            source.id_,
            'file',
            length=len(self.data),
            min_part_size=self.part_size,
            max_part_size=self.part_size,
            **kwargs,
        )
        assert file_version._type() == 'large'
        return file_version
