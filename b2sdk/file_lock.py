######################################################################
#
# File: b2sdk/file_lock.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional
import enum

from .exception import UnexpectedCloudBehaviour
# TODO: write __repr__ and __eq__ methods for the classes below

ACTIONS_WITHOUT_LOCK_SETTINGS = frozenset(['hide', 'folder'])


@enum.unique
class RetentionMode(enum.Enum):
    COMPLIANCE = "compliance"  # TODO: docs
    GOVERNANCE = "governance"  # TODO: docs
    NONE = None
    UNKNOWN = "unknown"


RETENTION_MODES_REQUIRING_PERIODS = frozenset({RetentionMode.COMPLIANCE, RetentionMode.GOVERNANCE})


class RetentionPeriod:
    def __init__(self, years: Optional[int] = None, days: Optional[int] = None):
        assert (years is None) != (days is None)
        if years is not None:
            self.duration = years
            self.unit = 'years'
        else:
            self.duration = days
            self.unit = 'days'

    @classmethod
    def from_period_dict(cls, period_dict):
        """
        Build a RetentionPeriod from an object returned by the server, such as:

        .. code-block ::

            {
                "duration": 2,
                "unit": "years"
            }
        """
        return cls(**{period_dict['unit']: period_dict['duration']})

    def as_dict(self):
        return {
            "duration": self.duration,
            "unit": self.unit,
        }


class FileRetentionSetting:
    def __init__(self, mode: RetentionMode, retain_until: Optional[int] = None):
        if mode in RETENTION_MODES_REQUIRING_PERIODS and retain_until is None:
            raise ValueError('must specify retain_until for retention mode %s' % (mode,))
        self.mode = mode
        self.retain_until = retain_until

    @classmethod
    def from_file_version_dict(cls, file_version_dict: dict):
        """
        Returns FileRetentionSetting for the given file_version_dict retrieved from the api. E.g.

        .. code-block ::

            {
                "action": "upload",
                "fileRetention": {
                    "isClientAuthorizedToRead": false,
                    "value": null
                },
                ...
            }

            {
                "action": "upload",
                "fileRetention": {
                    "isClientAuthorizedToRead": true,
                    "value": {
                      "mode": "governance",
                      "retainUntilTimestamp": 1628942493000
                    }
                },
                ...
            }
        """
        if 'fileRetention' not in file_version_dict:
            if file_version_dict['action'] not in ACTIONS_WITHOUT_LOCK_SETTINGS:
                raise UnexpectedCloudBehaviour(
                    'No fileRetention provided for file version with action=%s' %
                    (file_version_dict['action'])
                )
            return NO_RETENTION_FILE_SETTING

        retention_dict = file_version_dict['fileRetention']

        if not retention_dict['isClientAuthorizedToRead']:
            return cls(RetentionMode.UNKNOWN, None)

        mode = retention_dict['value']['mode']
        if mode is None:
            return NO_RETENTION_FILE_SETTING

        return cls(
            RetentionMode(mode),
            retention_dict['value']['retainUntilTimestamp'],
        )

    def serialize_to_json_for_request(self):
        if self.mode is RetentionMode.UNKNOWN:
            raise ValueError('cannot use an unknown file retention setting in requests')
        return self.as_dict()

    def as_dict(self):
        return {
            "mode": self.mode.value,
            "retainUntilTimestamp": self.retain_until,
        }

    def add_to_to_upload_headers(self, headers):
        if self.mode is RetentionMode.UNKNOWN:
            raise ValueError('cannot use an unknown file retention setting in requests')

        headers['X-Bz-File-Retention-Mode'] = str(
            self.mode.value
        )  # mode = NONE is not supported by the server at the
        # moment, but it should be
        headers['X-Bz-File-Retention-Retain-Until-Timestamp'] = self.retain_until


class LegalHoldSerializer:
    @classmethod
    def from_server(cls, file_version_dict) -> Optional[bool]:
        if 'legalHold' not in file_version_dict:
            if file_version_dict['action'] not in ACTIONS_WITHOUT_LOCK_SETTINGS:
                raise UnexpectedCloudBehaviour(
                    'legalHold not provided for file version with action=%s' %
                    (file_version_dict['action'])
                )
            return None
        legal_hold_dict = file_version_dict['legalHold']
        if legal_hold_dict['value'] is None:
            return None
        if legal_hold_dict['value'] == 'on':
            return True
        elif legal_hold_dict['value'] == 'off':
            return False
        raise ValueError('Unknown legal hold value: %s' % (legal_hold_dict['value'],))

    @classmethod
    def to_server(cls, bool_value: Optional[bool]) -> str:
        if bool_value is None:
            raise ValueError('Cannot use unknown legal hold in requests')
        if bool_value:
            return 'on'
        return 'off'

    @classmethod
    def add_to_upload_headers(cls, bool_value: Optional[bool], headers):
        headers['X-Bz-File-Legal-Hold'] = cls.to_server(bool_value)


class BucketRetentionSetting:
    def __init__(self, mode: RetentionMode, period: Optional[RetentionPeriod] = None):
        if mode in RETENTION_MODES_REQUIRING_PERIODS and period is None:
            raise ValueError('must specify period for retention mode %s' % (mode,))
        self.mode = mode
        self.period = period

    @classmethod
    def from_bucket_retention_dict(cls, retention_dict: dict):
        """
        Build a BucketRetentionSetting from an object returned by the server, such as:

        .. code-block::

            {
                "mode": "compliance",
                "period": {
                    "duration": 7,
                    "unit": "days"
                }
            }

        """
        period = retention_dict['period']
        if period is not None:
            period = RetentionPeriod.from_period_dict(period)
        return cls(RetentionMode(retention_dict['mode']), period)

    def as_dict(self):
        result = {
            'mode': self.mode.value,
        }
        if self.period is not None:
            result['period'] = self.period.as_dict()
        return result

    def serialize_to_json_for_request(self):
        if self.mode == RetentionMode.UNKNOWN:
            raise ValueError('cannot use an unknown file lock configuration in requests')
        return self.as_dict()


class FileLockConfiguration:
    def __init__(
        self,
        default_retention: BucketRetentionSetting,
        is_file_lock_enabled: Optional[bool],
    ):
        self.default_retention = default_retention
        self.is_file_lock_enabled = is_file_lock_enabled

    @classmethod
    def from_bucket_dict(cls, bucket_dict):
        """
        Build a FileLockConfiguration from an object returned by server, such as:

        .. code-block::
            {
                "isClientAuthorizedToRead": true,
                "value": {
                    "defaultRetention": {
                        "mode": "governance",
                        "period": {
                            "duration": 2,
                            "unit": "years"
                        }
                    },
                    "isFileLockEnabled": true
                }
            }

            or

            {
                "isClientAuthorizedToRead": false,
                "value": null
            }
        """

        if not bucket_dict['fileLockConfiguration']['isClientAuthorizedToRead']:
            return cls(UNKNOWN_BUCKET_RETENTION, None)
        retention = BucketRetentionSetting.from_bucket_retention_dict(
            bucket_dict['fileLockConfiguration']['value']['defaultRetention']
        )
        is_file_lock_enabled = bucket_dict['fileLockConfiguration']['value']['isFileLockEnabled']
        return cls(retention, is_file_lock_enabled)

    def as_dict(self):
        return {
            "defaultRetention": self.default_retention.as_dict(),
            "isFileLockEnabled": self.is_file_lock_enabled,
        }


UNKNOWN_BUCKET_RETENTION = BucketRetentionSetting(RetentionMode.UNKNOWN)
UNKNOWN_FILE_LOCK_CONFIGURATION = FileLockConfiguration(UNKNOWN_BUCKET_RETENTION, None)
NO_RETENTION_BUCKET_SETTING = BucketRetentionSetting(RetentionMode.NONE)
NO_RETENTION_FILE_SETTING = FileRetentionSetting(RetentionMode.NONE)
