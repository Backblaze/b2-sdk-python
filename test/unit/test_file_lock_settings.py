######################################################################
#
# File: test/unit/test_file_lock_settings.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Object Lock parsing and request contracts; no account or bucket operations.

References: https://www.backblaze.com/apidocs/b2-upload-file
https://www.backblaze.com/apidocs/b2-update-file-retention
"""

from __future__ import annotations

import pytest
from apiver_deps import (
    BucketRetentionSetting,
    FileLockConfiguration,
    FileRetentionSetting,
    LegalHold,
    RetentionMode,
    RetentionPeriod,
)
from apiver_deps_exception import UnexpectedCloudBehaviour

UNTIL = 1900000000123


@pytest.mark.parametrize('mode', [RetentionMode.GOVERNANCE, RetentionMode.COMPLIANCE])
def test_retention_requires_file_timestamp_and_bucket_period(mode):
    with pytest.raises(ValueError, match='retain_until'):
        FileRetentionSetting(mode)
    with pytest.raises(ValueError, match='period'):
        BucketRetentionSetting(mode)


@pytest.mark.parametrize(
    'mode,wire',
    [(RetentionMode.GOVERNANCE, 'governance'), (RetentionMode.COMPLIANCE, 'compliance')],
)
def test_file_retention_serializes_json_numbers_and_header_strings(mode, wire):
    setting = FileRetentionSetting(mode, UNTIL)
    assert setting.serialize_to_json_for_request() == {
        'mode': wire,
        'retainUntilTimestamp': UNTIL,
    }
    headers = {'Authorization': 'synthetic-token'}
    setting.add_to_to_upload_headers(headers)
    assert headers == {
        'Authorization': 'synthetic-token',
        'X-Bz-File-Retention-Mode': wire,
        'X-Bz-File-Retention-Retain-Until-Timestamp': str(UNTIL),
    }
    assert FileRetentionSetting.from_response_headers(headers) == setting


@pytest.mark.parametrize('operation', ['serialize_to_json_for_request', 'add_to_to_upload_headers'])
def test_unknown_retention_cannot_be_written(operation):
    setting = FileRetentionSetting(RetentionMode.UNKNOWN)
    with pytest.raises(ValueError, match='unknown file retention'):
        if operation == 'serialize_to_json_for_request':
            setting.serialize_to_json_for_request()
        else:
            headers = {'preserved': 'value'}
            setting.add_to_to_upload_headers(headers)
    if operation != 'serialize_to_json_for_request':
        assert headers == {'preserved': 'value'}


@pytest.mark.parametrize('action', ['hide', 'folder'])
def test_marker_versions_may_omit_lock_metadata(action):
    assert (
        FileRetentionSetting.from_file_version_dict({'action': action}).mode == RetentionMode.NONE
    )
    assert LegalHold.from_file_version_dict({'action': action}) == LegalHold.UNSET


@pytest.mark.parametrize('action', ['upload', 'start'])
@pytest.mark.parametrize(
    'reader', [FileRetentionSetting.from_file_version_dict, LegalHold.from_file_version_dict]
)
def test_data_versions_missing_lock_metadata_are_rejected(action, reader):
    with pytest.raises(UnexpectedCloudBehaviour):
        reader({'action': action})


@pytest.mark.parametrize('authorized', [False, True])
def test_retention_and_legal_hold_distinguish_unknown_from_unset(authorized):
    response = {
        'action': 'upload',
        'fileRetention': {
            'isClientAuthorizedToRead': authorized,
            'value': {'mode': None, 'retainUntilTimestamp': None} if authorized else None,
        },
        'legalHold': {'isClientAuthorizedToRead': authorized, 'value': None},
    }
    assert FileRetentionSetting.from_file_version_dict(response).mode == (
        RetentionMode.NONE if authorized else RetentionMode.UNKNOWN
    )
    assert LegalHold.from_file_version_dict(response) == (
        LegalHold.UNSET if authorized else LegalHold.UNKNOWN
    )


@pytest.mark.parametrize('mode', [RetentionMode.GOVERNANCE, RetentionMode.COMPLIANCE])
def test_authorized_file_response_retains_mode_and_timestamp(mode):
    value = {'mode': mode.value, 'retainUntilTimestamp': UNTIL}
    setting = FileRetentionSetting.from_file_version_dict(
        {
            'action': 'upload',
            'fileRetention': {'isClientAuthorizedToRead': True, 'value': value},
        }
    )
    assert setting.mode == mode
    assert setting.retain_until == UNTIL
    assert FileRetentionSetting.from_server_response({'fileRetention': value}) == setting


def test_unknown_bucket_retention_cannot_be_sent_in_requests():
    with pytest.raises(ValueError, match='unknown file lock'):
        BucketRetentionSetting(RetentionMode.UNKNOWN).serialize_to_json_for_request()


@pytest.mark.parametrize('unit', ['days', 'years'])
def test_bucket_period_has_one_explicit_unit(unit):
    period = RetentionPeriod(**{unit: 7})
    assert period.as_dict() == {'duration': 7, 'unit': unit}
    assert RetentionPeriod.from_period_dict({'duration': 7, 'unit': unit}) == period
    setting = BucketRetentionSetting(RetentionMode.GOVERNANCE, period)
    assert setting.serialize_to_json_for_request() == {
        'mode': 'governance',
        'period': {'duration': 7, 'unit': unit},
    }


@pytest.mark.parametrize('kwargs', [{}, {'days': 1, 'years': 1}])
def test_retention_period_requires_exactly_one_unit(kwargs):
    with pytest.raises(AssertionError):
        RetentionPeriod(**kwargs)


@pytest.mark.parametrize(
    'hold,wire', [(LegalHold.ON, 'on'), (LegalHold.OFF, 'off'), (LegalHold.UNSET, 'off')]
)
def test_legal_hold_wire_state_and_flags(hold, wire):
    assert hold.to_server() == wire
    assert hold.is_on() == (wire == 'on')
    assert hold.is_off() == (wire == 'off')
    headers = {'preserved': 'value'}
    hold.add_to_upload_headers(headers)
    assert headers == {'preserved': 'value', 'X-Bz-File-Legal-Hold': wire}


def test_unknown_legal_hold_is_not_sent_as_off():
    headers = {'preserved': 'value'}
    assert LegalHold.UNKNOWN.is_unknown()
    with pytest.raises(ValueError, match='unknown legal hold'):
        LegalHold.UNKNOWN.add_to_upload_headers(headers)
    assert headers == {'preserved': 'value'}


@pytest.mark.parametrize(
    'headers,mode,hold',
    [
        ({}, RetentionMode.NONE, LegalHold.UNSET),
        (
            {'X-Bz-Client-Unauthorized-To-Read': 'X-Bz-File-Retention-Mode,X-Bz-File-Legal-Hold'},
            RetentionMode.UNKNOWN,
            LegalHold.UNKNOWN,
        ),
        (
            {
                'X-Bz-File-Retention-Mode': 'governance',
                'X-Bz-File-Retention-Retain-Until-Timestamp': str(UNTIL),
                'X-Bz-File-Legal-Hold': 'on',
            },
            RetentionMode.GOVERNANCE,
            LegalHold.ON,
        ),
    ],
)
def test_response_headers_distinguish_absent_unreadable_and_enabled(headers, mode, hold):
    setting = FileRetentionSetting.from_response_headers(headers)
    assert setting.mode == mode
    assert setting.retain_until == (UNTIL if mode == RetentionMode.GOVERNANCE else None)
    assert LegalHold.from_response_headers(headers) == hold


@pytest.mark.parametrize(
    'headers',
    [
        {'X-Bz-File-Retention-Mode': 'governance'},
        {
            'X-Bz-File-Retention-Mode': 'governance',
            'X-Bz-File-Retention-Retain-Until-Timestamp': 'invalid',
        },
        {'X-Bz-File-Retention-Mode': 'unrecognized'},
    ],
)
def test_malformed_retention_headers_are_not_treated_as_no_retention(headers):
    with pytest.raises(ValueError):
        FileRetentionSetting.from_response_headers(headers)


@pytest.mark.parametrize('authorized', [False, True])
def test_bucket_lock_configuration_preserves_unknown_state(authorized):
    response = {
        'fileLockConfiguration': {
            'isClientAuthorizedToRead': authorized,
            'value': {
                'isFileLockEnabled': True,
                'defaultRetention': {
                    'mode': 'governance',
                    'period': {'duration': 7, 'unit': 'days'},
                },
            }
            if authorized
            else None,
        }
    }
    setting = FileLockConfiguration.from_bucket_dict(response)
    assert setting.is_file_lock_enabled is (True if authorized else None)
    assert setting.default_retention.mode == (
        RetentionMode.GOVERNANCE if authorized else RetentionMode.UNKNOWN
    )
    expected = {
        'defaultRetention': {'mode': 'governance', 'period': {'duration': 7, 'unit': 'days'}}
        if authorized
        else {'mode': 'unknown'},
        'isFileLockEnabled': True if authorized else None,
    }
    assert setting.as_dict() == expected
    assert setting == FileLockConfiguration.from_bucket_dict(response)
