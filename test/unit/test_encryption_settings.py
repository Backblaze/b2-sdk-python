######################################################################
#
# File: test/unit/test_encryption_settings.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Encryption state and wire-format contracts using public, synthetic key material.

Reference: https://www.backblaze.com/docs/cloud-storage-server-side-encryption-with-the-native-api
"""

from __future__ import annotations

import pytest
from apiver_deps import (
    UNKNOWN_KEY_ID,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    EncryptionSettingFactory,
)

KEY = bytes(range(32))
KEY_B64 = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8='
KEY_MD5 = 'tP/LI3N87DFaSk0aoqYgzg=='
CUSTOMER_HEADERS = {
    'X-Bz-Server-Side-Encryption-Customer-Algorithm': 'AES256',
    'X-Bz-Server-Side-Encryption-Customer-Key': KEY_B64,
    'X-Bz-Server-Side-Encryption-Customer-Key-Md5': KEY_MD5,
}


def customer_setting(key_id='key id+Δ', secret=KEY):
    return EncryptionSetting(
        EncryptionMode.SSE_C, EncryptionAlgorithm.AES256, EncryptionKey(secret, key_id)
    )


@pytest.mark.parametrize(
    'kwargs',
    [
        {'mode': EncryptionMode.NONE, 'algorithm': EncryptionAlgorithm.AES256},
        {'mode': EncryptionMode.NONE, 'key': EncryptionKey(KEY, None)},
        {'mode': EncryptionMode.SSE_B2},
        {'mode': EncryptionMode.SSE_C},
        {'mode': EncryptionMode.SSE_C, 'algorithm': EncryptionAlgorithm.AES256},
    ],
)
def test_encryption_constructor_rejects_incomplete_or_conflicting_settings(kwargs):
    with pytest.raises(ValueError):
        EncryptionSetting(**kwargs)


@pytest.mark.parametrize('key_id', [None, UNKNOWN_KEY_ID, 'public-id'])
def test_key_representation_never_discloses_secret(key_id):
    key = EncryptionKey(KEY, key_id)
    text = repr(key)
    assert str(KEY) not in text
    assert KEY_B64 not in text
    assert KEY_MD5 not in text
    assert '******' in text
    if key_id is UNKNOWN_KEY_ID:
        assert 'unknown' in text


def test_server_key_metadata_has_no_usable_secret():
    key = EncryptionKey(None, UNKNOWN_KEY_ID)
    assert key.as_dict() == {'customerKey': '******', 'customerKeyMd5': '******'}
    assert 'None' in repr(key)
    assert 'unknown' in repr(key)


@pytest.mark.parametrize('key_id', [None, 'public-id'])
def test_customer_key_serialization_uses_known_base64_vectors(key_id):
    setting = customer_setting(key_id)
    assert setting.serialize_to_json_for_request() == {
        'mode': 'SSE-C',
        'algorithm': 'AES256',
        'customerKey': KEY_B64,
        'customerKeyMd5': KEY_MD5,
    }
    assert setting.key.key_b64() == KEY_B64
    assert setting.key.key_md5() == KEY_MD5


@pytest.mark.parametrize('operation', ['serialize_to_json_for_request', 'add_to_download_headers'])
def test_unknown_customer_secret_cannot_be_used_in_a_request(operation):
    setting = customer_setting(secret=None)
    with pytest.raises(ValueError, match='unknown key'):
        if operation == 'serialize_to_json_for_request':
            setting.serialize_to_json_for_request()
        else:
            setting.add_to_download_headers({})


@pytest.mark.parametrize('direction', ['upload', 'download'])
@pytest.mark.parametrize('mode', [EncryptionMode.SSE_B2, EncryptionMode.SSE_C])
def test_encryption_headers_preserve_existing_headers(direction, mode):
    setting = (
        customer_setting()
        if mode == EncryptionMode.SSE_C
        else EncryptionSetting(mode, EncryptionAlgorithm.AES256)
    )
    headers = {'Authorization': 'synthetic-token'}
    getattr(setting, f'add_to_{direction}_headers')(headers)
    expected = {'Authorization': 'synthetic-token'}
    if mode == EncryptionMode.SSE_C:
        expected.update(CUSTOMER_HEADERS)
        if direction == 'upload':
            expected['X-Bz-Info-sse_c_key_id'] = 'key%20id%2B%CE%94'
    else:
        expected['X-Bz-Server-Side-Encryption'] = 'AES256'
    assert headers == expected


def test_no_encryption_download_preserves_headers_without_adding_sse():
    headers = {'Authorization': 'synthetic-token'}
    EncryptionSetting(EncryptionMode.NONE).add_to_download_headers(headers)
    assert headers == {'Authorization': 'synthetic-token'}


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_unknown_encryption_mode_cannot_generate_headers(direction):
    with pytest.raises(NotImplementedError, match='unsupported encryption setting'):
        getattr(EncryptionSetting(EncryptionMode.UNKNOWN), f'add_to_{direction}_headers')({})


@pytest.mark.parametrize('operation', ['add_to_upload_headers', 'add_key_id_to_file_info'])
def test_unknown_key_identity_is_rejected_for_metadata_writes(operation):
    with pytest.raises(ValueError, match='unknown key id'):
        getattr(customer_setting(UNKNOWN_KEY_ID), operation)({})


def test_conflicting_key_identity_is_not_silently_overwritten():
    with pytest.raises(ValueError, match='Ambiguous key id'):
        customer_setting('new').add_to_upload_headers({'X-Bz-Info-sse_c_key_id': 'old'})
    info = {'sse_c_key_id': 'old', 'other': 'unchanged'}
    with pytest.raises(ValueError, match='Ambiguous key id'):
        customer_setting('new').add_key_id_to_file_info(info)
    assert info == {'sse_c_key_id': 'old', 'other': 'unchanged'}


@pytest.mark.parametrize('info', [None, {}, {'sse_c_key_id': 'new', 'other': 'kept'}])
def test_key_identity_is_added_to_file_metadata(info):
    expected = {**(info or {}), 'sse_c_key_id': 'new'}
    assert customer_setting('new').add_key_id_to_file_info(info) == expected


@pytest.mark.parametrize('mode', [EncryptionMode.NONE, EncryptionMode.SSE_B2])
def test_settings_without_customer_key_leave_file_metadata_alone(mode):
    setting = (
        EncryptionSetting(mode)
        if mode == EncryptionMode.NONE
        else EncryptionSetting(mode, EncryptionAlgorithm.AES256)
    )
    info = {'other': 'kept'}
    assert setting.add_key_id_to_file_info(info) is info
    assert info == {'other': 'kept'}


@pytest.mark.parametrize(
    'metadata,mode,algorithm,key_id',
    [
        ({}, EncryptionMode.NONE, None, None),
        ({'serverSideEncryption': {'mode': None}}, EncryptionMode.NONE, None, None),
        (
            {'serverSideEncryption': {'mode': 'SSE-B2', 'algorithm': 'AES256'}},
            EncryptionMode.SSE_B2,
            EncryptionAlgorithm.AES256,
            None,
        ),
        (
            {
                'serverSideEncryption': {'mode': 'SSE-C', 'algorithm': 'AES256'},
                'fileInfo': {'sse_c_key_id': 'key%20id%2B%CE%94'},
            },
            EncryptionMode.SSE_C,
            EncryptionAlgorithm.AES256,
            'key id+Δ',
        ),
    ],
)
def test_response_metadata_preserves_encryption_state(metadata, mode, algorithm, key_id):
    setting = EncryptionSettingFactory.from_file_version_dict(metadata)
    assert setting.mode == mode
    assert setting.algorithm == algorithm
    if mode == EncryptionMode.SSE_C:
        assert setting.key.secret is None
        assert setting.key.key_id == key_id
    else:
        assert setting.key is None


@pytest.mark.parametrize(
    'metadata,mode',
    [
        ({}, EncryptionMode.UNKNOWN),
        (
            {'defaultServerSideEncryption': {'isClientAuthorizedToRead': False}},
            EncryptionMode.UNKNOWN,
        ),
        (
            {'defaultServerSideEncryption': {'isClientAuthorizedToRead': True, 'value': None}},
            EncryptionMode.NONE,
        ),
        (
            {
                'defaultServerSideEncryption': {
                    'isClientAuthorizedToRead': True,
                    'value': {'mode': 'SSE-B2', 'algorithm': 'AES256'},
                }
            },
            EncryptionMode.SSE_B2,
        ),
    ],
)
def test_bucket_response_distinguishes_unreadable_from_unencrypted(metadata, mode):
    assert EncryptionSettingFactory.from_bucket_dict(metadata).mode == mode


@pytest.mark.parametrize(
    'metadata',
    [
        {'serverSideEncryption': {'mode': 'unrecognized'}},
        {'serverSideEncryption': {'mode': 'SSE-B2', 'algorithm': 'unrecognized'}},
    ],
)
def test_unrecognized_response_encryption_is_not_treated_as_plaintext(metadata):
    with pytest.raises(ValueError):
        EncryptionSettingFactory.from_file_version_dict(metadata)


@pytest.mark.parametrize(
    'key_id,existing',
    [
        ('key id+Δ', 'key%20id%2B%CE%94'),
        ('key id+Δ', 'key%20id%2b%ce%94'),
        ('literal%20value', 'literal%2520value'),
    ],
)
def test_equivalent_encoded_key_id_headers_are_accepted_without_double_encoding(key_id, existing):
    headers = {'X-Bz-Info-sse_c_key_id': existing}
    setting = customer_setting(key_id)
    setting.add_to_upload_headers(headers)
    setting.add_to_upload_headers(headers)
    assert headers['X-Bz-Info-sse_c_key_id'] == (
        'literal%2520value' if key_id == 'literal%20value' else 'key%20id%2B%CE%94'
    )
    for name, value in CUSTOMER_HEADERS.items():
        assert headers[name] == value
