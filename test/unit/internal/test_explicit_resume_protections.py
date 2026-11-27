######################################################################
#
# File: test/unit/internal/test_explicit_resume_protections.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import hashlib
import io
from unittest import mock

import pytest

from b2sdk.v3 import (
    NO_RETENTION_FILE_SETTING,
    SSE_B2_AES,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FileRetentionSetting,
    LegalHold,
    RetentionMode,
    UploadSourceBytes,
    WriteIntent,
)

RETENTION = FileRetentionSetting(RetentionMode.COMPLIANCE, 4102444800000)
SSE_C = EncryptionSetting(
    EncryptionMode.SSE_C, EncryptionAlgorithm.AES256, EncryptionKey(b'x' * 32, None)
)


@pytest.fixture
def prepared_resume(b2api):
    bucket = b2api.create_bucket('resume-bucket', 'allPrivate', is_file_lock_enabled=True)
    data = b'a' * 600

    def prepare(encryption=SSE_B2_AES, retention=None, hold=None):
        unfinished = b2api.session.start_large_file(
            bucket.id_,
            'destination',
            'application/octet-stream',
            {'large_file_sha1': hashlib.sha1(data).hexdigest()},
            server_side_encryption=encryption,
            file_retention=retention,
            legal_hold=hold,
        )
        for number in range(1, 4):
            part = data[(number - 1) * 200 : number * 200]
            b2api.session.upload_part(
                unfinished['fileId'],
                number,
                len(part),
                hashlib.sha1(part).hexdigest(),
                io.BytesIO(part),
                server_side_encryption=encryption,
            )
        return bucket, data, unfinished['fileId']

    return prepare


@pytest.mark.parametrize(
    'requested,existing,field',
    [
        ({'encryption': SSE_C}, {}, 'encryption'),
        ({'file_retention': RETENTION}, {}, 'file_retention'),
        ({'legal_hold': LegalHold.ON}, {}, 'legal_hold'),
        (
            {'encryption': SSE_C, 'file_retention': RETENTION, 'legal_hold': LegalHold.ON},
            {},
            'encryption',
        ),
        ({'legal_hold': LegalHold.OFF}, {}, 'legal_hold'),
    ],
    ids=[
        'encryption',
        'retention',
        'hold',
        'combined',
        'off-vs-unset',
    ],
)
def test_explicit_resume_rejects_incompatible_protections(
    prepared_resume, requested, existing, field
):
    bucket, data, file_id = prepared_resume(**existing)
    session = bucket.api.session
    with mock.patch.object(session, 'finish_large_file', wraps=session.finish_large_file) as finish:
        with pytest.raises(ValueError, match=field):
            bucket.create_file(
                [WriteIntent(UploadSourceBytes(data))],
                'destination',
                continue_large_file_id=file_id,
                max_part_size=200,
                **requested,
            )
        finish.assert_not_called()
    remaining = list(bucket.list_unfinished_large_files())
    assert [item.file_id for item in remaining] == [file_id]
    assert len(list(bucket.api.services.large_file.list_parts(file_id))) == 3


@pytest.mark.parametrize(
    'retention,hold',
    [
        (None, None),
        pytest.param(
            NO_RETENTION_FILE_SETTING,
            LegalHold.UNSET,
            marks=pytest.mark.rawsimulator(
                reason='Backblaze/b2-sdk-python#774: LegalHold.UNSET is sent as "off", so the '
                'server stores off and the explicit resume refuses UNSET against it'
            ),
        ),
        (RETENTION, LegalHold.ON),
    ],
)
def test_explicit_resume_accepts_matching_protections(prepared_resume, retention, hold):
    bucket, data, file_id = prepared_resume(retention=retention, hold=hold)
    result = bucket.create_file(
        [WriteIntent(UploadSourceBytes(data))],
        'destination',
        continue_large_file_id=file_id,
        max_part_size=200,
        encryption=SSE_B2_AES,
        file_retention=retention,
        legal_hold=hold,
    )
    assert result.id_ == file_id
    assert result.server_side_encryption == SSE_B2_AES
    assert result.file_retention == (retention or NO_RETENTION_FILE_SETTING)
    assert result.legal_hold == (hold or LegalHold.UNSET)


@pytest.mark.parametrize(
    'existing',
    [{'retention': RETENTION}, {'hold': LegalHold.ON}],
    ids=['omitted-retention', 'omitted-hold'],
)
def test_explicit_resume_allows_omitted_protections(prepared_resume, existing):
    # Omitting file_retention / legal_hold is "not requested", not a conflicting NONE/UNSET,
    # so the resume is allowed and the unfinished file keeps its existing protection.
    bucket, data, file_id = prepared_resume(**existing)
    result = bucket.create_file(
        [WriteIntent(UploadSourceBytes(data))],
        'destination',
        continue_large_file_id=file_id,
        max_part_size=200,
    )
    assert result.id_ == file_id


@pytest.mark.parametrize(
    'field,unknown,requested',
    [
        ('file_retention', FileRetentionSetting(RetentionMode.UNKNOWN), RETENTION),
        ('legal_hold', LegalHold.UNKNOWN, LegalHold.ON),
    ],
)
def test_explicit_resume_rejects_unreadable_protections(prepared_resume, field, unknown, requested):
    bucket, data, file_id = prepared_resume()
    manager = bucket.api.services.large_file
    unfinished = manager.get_unfinished_large_file(bucket.id_, file_id)
    setattr(unfinished, field, unknown)
    with mock.patch.object(manager, 'get_unfinished_large_file', return_value=unfinished):
        with pytest.raises(ValueError, match=field):
            bucket.create_file(
                [WriteIntent(UploadSourceBytes(data))],
                'destination',
                continue_large_file_id=file_id,
                max_part_size=200,
                **{field: requested},
            )
    assert [item.file_id for item in bucket.list_unfinished_large_files()] == [file_id]
