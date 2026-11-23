######################################################################
#
# File: test/unit/bucket/test_copy_header_content_type.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from hashlib import sha1
from unittest import mock

import pytest

from b2sdk._internal.exception import AccessDenied
from b2sdk._internal.http_constants import LARGE_FILE_SHA1, SSE_C_KEY_ID_FILE_INFO_KEY_NAME

from .test_bucket import SSE_C_AES, SSE_C_AES_2


@pytest.mark.apiver(from_ver=2)
@pytest.mark.parametrize('length', [None, 5])
@pytest.mark.parametrize('provide_source_metadata', [False, True])
def test_copy_headers_without_content_type_preserves_source_metadata(
    bucket, length, provide_source_metadata, monkeypatch
):
    source = bucket.upload_bytes(
        b'hello', 'source.txt', content_type='application/x-example', file_info={'owner': 'alice'}
    )
    source_metadata = (
        {'source_content_type': source.content_type, 'source_file_info': source.file_info}
        if provide_source_metadata
        else {}
    )
    if provide_source_metadata:

        def fail_lookup(*args, **kwargs):
            raise AssertionError('source metadata lookup was unnecessary')

        monkeypatch.setattr(bucket.api.session, 'get_file_info_by_id', fail_lookup)
    copied = bucket.copy(
        source.id_,
        'destination.txt',
        length=length,
        expires='Thu, 01 Dec 2050 16:00:00 GMT',
        content_language='en',
        **source_metadata,
    )
    assert copied.content_type == 'application/x-example'
    assert copied.file_info['owner'] == 'alice'
    assert copied.file_info['b2-expires'] == 'Thu, 01 Dec 2050 16:00:00 GMT'
    assert copied.file_info['b2-content-language'] == 'en'


@pytest.mark.apiver(from_ver=2)
def test_copy_headers_uses_explicit_destination_metadata(bucket, monkeypatch):
    source = bucket.upload_bytes(
        b'hello', 'source.txt', content_type='application/x-example', file_info={'old': 'value'}
    )

    def fail_lookup(*args, **kwargs):
        raise AssertionError('source metadata lookup was unnecessary')

    monkeypatch.setattr(bucket.api.session, 'get_file_info_by_id', fail_lookup)
    copied = bucket.copy(
        source.id_,
        'destination.txt',
        source_content_type='application/x-example',
        file_info={'new': 'value'},
        content_disposition='attachment',
    )
    assert copied.content_type == 'application/x-example'
    assert copied.file_info == {'new': 'value', 'b2-content-disposition': 'attachment'}


@pytest.mark.apiver(from_ver=2)
@pytest.mark.parametrize('length', [None, 5])
@pytest.mark.parametrize('destination_encryption', [None, SSE_C_AES_2])
def test_copy_headers_replaces_source_encryption_key_id(bucket, length, destination_encryption):
    source = bucket.upload_bytes(
        b'hello',
        'source.txt',
        content_type='application/x-example',
        file_info={'owner': 'alice'},
        encryption=SSE_C_AES,
    )
    copied = bucket.copy(
        source.id_,
        'destination.txt',
        length=length,
        source_encryption=SSE_C_AES,
        destination_encryption=destination_encryption,
        source_file_info=source.file_info,
        source_content_type=source.content_type,
        content_language='en',
    )
    assert copied.file_info['owner'] == 'alice'
    assert copied.file_info['b2-content-language'] == 'en'
    assert copied.file_info.get(SSE_C_KEY_ID_FILE_INFO_KEY_NAME) == (
        destination_encryption.key.key_id if destination_encryption else None
    )
    assert source.file_info[SSE_C_KEY_ID_FILE_INFO_KEY_NAME] == SSE_C_AES.key.key_id


@pytest.mark.apiver(from_ver=2)
@pytest.mark.parametrize('provide_source_metadata', [False, True])
def test_partial_multipart_copy_does_not_inherit_source_checksum(bucket, provide_source_metadata):
    data = b'a' * 300 + b'b' * 300
    source_checksum = sha1(data).hexdigest()
    source = bucket.upload_bytes(
        data,
        'source.txt',
        content_type='application/x-example',
        file_info={'owner': 'alice'},
        large_file_sha1=source_checksum,
    )
    source_metadata = (
        {'source_content_type': source.content_type, 'source_file_info': source.file_info}
        if provide_source_metadata
        else {}
    )
    with mock.patch.object(
        bucket.api.session, 'copy_part', wraps=bucket.api.session.copy_part
    ) as copy_part:
        copied = bucket.copy(
            source.id_,
            'partial.txt',
            offset=100,
            length=400,
            max_part_size=200,
            content_language='en',
            **source_metadata,
        )
    assert copy_part.call_count == 2
    assert copied.size == 400
    assert LARGE_FILE_SHA1 not in copied.file_info
    assert copied.get_content_sha1() is None
    assert copied.file_info['owner'] == 'alice'
    assert copied.file_info['b2-content-language'] == 'en'
    assert source.file_info[LARGE_FILE_SHA1] == source_checksum


@pytest.mark.apiver(from_ver=2)
def test_whole_file_copy_keeps_source_checksum(bucket):
    data = b'hello'
    checksum = sha1(data).hexdigest()
    source = bucket.upload_bytes(data, 'source.txt', file_info={LARGE_FILE_SHA1: checksum})
    copied = bucket.copy(source.id_, 'destination.txt', content_language='en')
    assert copied.file_info[LARGE_FILE_SHA1] == checksum


@pytest.mark.apiver(from_ver=2)
def test_partial_copy_keeps_explicit_destination_checksum(bucket):
    data = b'a' * 300 + b'b' * 300
    source = bucket.upload_bytes(data, 'source.txt')
    checksum = sha1(data[100:500]).hexdigest()
    copied = bucket.copy(
        source.id_,
        'partial.txt',
        offset=100,
        length=400,
        max_part_size=200,
        source_content_type=source.content_type,
        file_info={LARGE_FILE_SHA1: checksum},
        content_language='en',
    )
    assert copied.file_info[LARGE_FILE_SHA1] == checksum
    assert copied.get_content_sha1() == checksum


@pytest.mark.apiver(from_ver=2)
def test_copy_headers_propagates_source_info_access_denied(bucket, monkeypatch):
    source = bucket.upload_bytes(b'hello', 'source.txt')

    def deny_lookup(*args, **kwargs):
        raise AccessDenied()

    monkeypatch.setattr(bucket.api.session, 'get_file_info_by_id', deny_lookup)
    with mock.patch.object(
        bucket.api.session, 'copy_file', wraps=bucket.api.session.copy_file
    ) as copy_file:
        with pytest.raises(AccessDenied):
            bucket.copy(source.id_, 'destination.txt', content_language='en')
    copy_file.assert_not_called()


@pytest.mark.apiver(from_ver=2)
def test_copy_documents_omission_of_source_checksum_for_partial_copies(bucket):
    doc = bucket.copy.__doc__
    assert 'partial cop' in doc.lower()
    assert 'large_file_sha1' in doc
