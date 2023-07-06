######################################################################
#
# File: test/unit/v0/test_copy_manager.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk.http_constants import SSE_C_KEY_ID_FILE_INFO_KEY_NAME
from b2sdk.transfer.outbound.copy_manager import CopyManager

from ..test_base import TestBase
from .deps import (
    SSE_B2_AES,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    MetadataDirectiveMode,
)
from .deps_exception import SSECKeyIdMismatchInCopy

SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'some_key', key_id='some-id'),
)
SSE_C_AES_2 = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'some_other_key', key_id='some-id-2'),
)


class TestCopyManager(TestBase):
    def test_establish_sse_c_replace(self):
        file_info = {'some_key': 'some_value'}
        content_type = 'text/plain'
        metadata_directive, new_file_info, new_content_type = CopyManager.establish_sse_c_file_metadata(
            MetadataDirectiveMode.REPLACE,
            destination_file_info=file_info,
            destination_content_type=content_type,
            destination_server_side_encryption=SSE_C_AES,
            source_server_side_encryption=SSE_C_AES_2,
            source_file_info=None,
            source_content_type=None,
        )
        self.assertEqual(
            (
                MetadataDirectiveMode.REPLACE, {
                    'some_key': 'some_value',
                    SSE_C_KEY_ID_FILE_INFO_KEY_NAME: 'some-id'
                }, content_type
            ), (metadata_directive, new_file_info, new_content_type)
        )

    def test_establish_sse_c_copy_no_enc(self):
        file_info = {}
        content_type = 'text/plain'
        metadata_directive, new_file_info, new_content_type = CopyManager.establish_sse_c_file_metadata(
            MetadataDirectiveMode.COPY,
            destination_file_info=file_info,
            destination_content_type=content_type,
            destination_server_side_encryption=None,
            source_server_side_encryption=None,
            source_file_info=None,
            source_content_type=None,
        )
        self.assertEqual(
            (MetadataDirectiveMode.COPY, {}, content_type),
            (metadata_directive, new_file_info, new_content_type)
        )

    def test_establish_sse_c_copy_b2(self):
        file_info = {}
        content_type = 'text/plain'
        metadata_directive, new_file_info, new_content_type = CopyManager.establish_sse_c_file_metadata(
            MetadataDirectiveMode.COPY,
            destination_file_info=file_info,
            destination_content_type=content_type,
            destination_server_side_encryption=SSE_B2_AES,
            source_server_side_encryption=None,
            source_file_info=None,
            source_content_type=None,
        )
        self.assertEqual(
            (MetadataDirectiveMode.COPY, {}, content_type),
            (metadata_directive, new_file_info, new_content_type)
        )

    def test_establish_sse_c_copy_same_key_id(self):
        file_info = None
        content_type = 'text/plain'
        metadata_directive, new_file_info, new_content_type = CopyManager.establish_sse_c_file_metadata(
            MetadataDirectiveMode.COPY,
            destination_file_info=file_info,
            destination_content_type=content_type,
            destination_server_side_encryption=SSE_C_AES,
            source_server_side_encryption=SSE_C_AES,
            source_file_info=None,
            source_content_type=None,
        )
        self.assertEqual(
            (MetadataDirectiveMode.COPY, None, content_type),
            (metadata_directive, new_file_info, new_content_type)
        )

    def test_establish_sse_c_copy_sources_given(self):
        metadata_directive, new_file_info, new_content_type = CopyManager.establish_sse_c_file_metadata(
            MetadataDirectiveMode.COPY,
            destination_file_info=None,
            destination_content_type=None,
            destination_server_side_encryption=SSE_C_AES,
            source_server_side_encryption=SSE_C_AES_2,
            source_file_info={
                'some_key': 'some_value',
                SSE_C_KEY_ID_FILE_INFO_KEY_NAME: 'some-id-2'
            },
            source_content_type='text/plain',
        )
        self.assertEqual(
            (
                MetadataDirectiveMode.REPLACE, {
                    'some_key': 'some_value',
                    SSE_C_KEY_ID_FILE_INFO_KEY_NAME: 'some-id'
                }, 'text/plain'
            ), (metadata_directive, new_file_info, new_content_type)
        )

    def test_establish_sse_c_copy_sources_unknown(self):
        for source_file_info, source_content_type in [
            (None, None),
            ({
                'a': 'b'
            }, None),
            (None, 'text/plain'),
        ]:
            with self.subTest(
                source_file_info=source_file_info, source_content_type=source_content_type
            ):
                with self.assertRaises(
                    SSECKeyIdMismatchInCopy,
                    'attempting to copy file using MetadataDirectiveMode.COPY without providing source_file_info '
                    'and source_content_type for differing sse_c_key_ids: source="some-id-2", destination="some-id"'
                ):
                    CopyManager.establish_sse_c_file_metadata(
                        MetadataDirectiveMode.COPY,
                        destination_file_info=None,
                        destination_content_type=None,
                        destination_server_side_encryption=SSE_C_AES,
                        source_server_side_encryption=SSE_C_AES_2,
                        source_file_info=source_file_info,
                        source_content_type=source_content_type,
                    )
