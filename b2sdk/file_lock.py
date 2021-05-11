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


@enum.unique
class RetentionMode(enum.Enum):
    COMPLIANCE = "compliance"  # TODO: docs
    GOVERNANCE = "governance"  # TODO: docs
    NONE = None
    UNKNOWN = "unknown"


RETENTION_MODES_REQUIRING_PERIODS = frozenset({RetentionMode.COMPLIANCE, RetentionMode.GOVERNANCE})


class RetentionPeriod:
    """
    "period": {
      "duration": 2,
      "unit": "years"
    }
    """

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
    def __init__(self, mode: RetentionMode, retain_until: Optional[int]):
        if mode in RETENTION_MODES_REQUIRING_PERIODS and retain_until is None:
            raise ValueError('must specify retain_until for retention mode %s' % (mode,))
        self.mode = mode
        self.retain_until = retain_until

    @classmethod
    def from_file_retention_dict(cls, retention_dict: dict):
        """
        Returns FileRetentionSetting for the given retention_dict retrieved from the api. E.g.

        .. code-block ::

            {
                "isClientAuthorizedToRead": false,
                "value": null
            }

            {
                "isClientAuthorizedToRead": true,
                "value": {
                  "mode": "governance",
                  "retainUntilTimestamp": 1628942493000
                }
            }
        """
        if retention_dict['value'] is None:
            return cls(RetentionMode.UNKNOWN, None)
        return cls(
            RetentionMode(retention_dict['value']['mode'] or 'none'),
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
    def from_server(cls, legal_hold_dict) -> Optional[bool]:
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
    """
    "defaultRetention": {
        "mode": "compliance",
        "period": {
          "duration": 7,
          "unit": "days"
        }
      }
    """

    def __init__(self, mode: RetentionMode, period: Optional[RetentionPeriod]):
        if mode in RETENTION_MODES_REQUIRING_PERIODS and period is None:
            raise ValueError('must specify period for retention mode %s' % (mode,))
        self.mode = mode
        self.period = period

    @classmethod
    def from_bucket_retention_dict(cls, retention_dict: dict):
        period = retention_dict['period']
        if period is not None:
            period = RetentionPeriod.from_period_dict(period)
        return cls(RetentionMode(retention_dict['mode'] or 'none'), period)

    def as_dict(self):
        if self.period is None:
            period_repr = None
        else:
            period_repr = self.period.as_dict()
        return {
            'mode': self.mode.value,
            'period': period_repr,
        }


class FileLockConfiguration:
    """
    "fileLockConfiguration": {
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

      "fileLockConfiguration": {
        "isClientAuthorizedToRead": false,
        "value": null
      }
    """

    def __init__(
        self,
        default_retention: BucketRetentionSetting,
        is_file_lock_enabled: Optional[bool],
    ):
        self.default_retention = default_retention
        self.is_file_lock_enabled = is_file_lock_enabled

    @classmethod
    def from_bucket_dict(cls, bucket_dict):
        if bucket_dict['fileLockConfiguration']['value'] is None:
            return cls(UNKNOWN_BUCKET_RETENTION, None)
        retention = BucketRetentionSetting.from_bucket_retention_dict(
            bucket_dict['fileLockConfiguration']['value']['defaultRetention']
        )
        is_file_lock_enabled = bucket_dict['fileLockConfiguration']['value']['isFileLockEnabled']
        return cls(retention, is_file_lock_enabled)

    def serialize_to_json_for_request(self):
        if self.is_file_lock_enabled is None:
            raise ValueError('cannot use an unknown file lock configuration in requests')
        return self.as_dict()

    def as_dict(self):
        return {
            "defaultRetention": self.default_retention.as_dict(),
            "isFileLockEnabled": self.is_file_lock_enabled
        }


UNKNOWN_BUCKET_RETENTION = BucketRetentionSetting(RetentionMode.UNKNOWN, None)
UNKNOWN_FILE_LOCK_CONFIGURATION = FileLockConfiguration(UNKNOWN_BUCKET_RETENTION, None)
NO_RETENTION_BUCKET_SETTING = BucketRetentionSetting(RetentionMode.NONE, None)
NO_RETENTION_FILE_SETTING = FileRetentionSetting(RetentionMode.NONE, None)
