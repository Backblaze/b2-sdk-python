######################################################################
#
# File: b2sdk/_internal/file_lock.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import enum

from .exception import UnexpectedCloudBehaviour

ACTIONS_WITHOUT_LOCK_SETTINGS = frozenset(['hide', 'folder'])


@enum.unique
class RetentionMode(enum.Enum):
    """Enum class representing retention modes set in files and buckets"""
    GOVERNANCE = "governance"  #: retention settings for files in this mode can be modified by clients with appropriate application key capabilities
    COMPLIANCE = "compliance"  #: retention settings for files in this mode can only be modified by extending the retention dates by clients with appropriate application key capabilities
    NONE = None  #: retention not set
    UNKNOWN = "unknown"  #: the client is not authorized to read retention settings


RETENTION_MODES_REQUIRING_PERIODS = frozenset({RetentionMode.COMPLIANCE, RetentionMode.GOVERNANCE})


class RetentionPeriod:
    """Represent a time period (either in days or in years) that is used as a default for bucket retention"""
    KNOWN_UNITS = ['days', 'years']

    def __init__(self, years: int | None = None, days: int | None = None):
        """Create a retention period, provide exactly one of: days, years"""
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
        assert period_dict['unit'] in cls.KNOWN_UNITS
        return cls(**{period_dict['unit']: period_dict['duration']})

    def as_dict(self):
        return {
            "duration": self.duration,
            "unit": self.unit,
        }

    def __repr__(self):
        return f'{self.__class__.__name__}({self.duration} {self.unit})'

    def __eq__(self, other):
        return self.unit == other.unit and self.duration == other.duration


class FileRetentionSetting:
    """Represent file retention settings, i.e. whether the file is retained, in which mode and until when"""

    def __init__(self, mode: RetentionMode, retain_until: int | None = None):
        if mode in RETENTION_MODES_REQUIRING_PERIODS and retain_until is None:
            raise ValueError(f'must specify retain_until for retention mode {mode}')
        self.mode = mode
        self.retain_until = retain_until

    @classmethod
    def from_file_version_dict(cls, file_version_dict: dict) -> FileRetentionSetting:
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
        file_retention_dict = file_version_dict['fileRetention']

        if not file_retention_dict['isClientAuthorizedToRead']:
            return cls(RetentionMode.UNKNOWN, None)

        return cls.from_file_retention_value_dict(file_retention_dict['value'])

    @classmethod
    def from_file_retention_value_dict(
        cls, file_retention_value_dict: dict
    ) -> FileRetentionSetting:

        mode = file_retention_value_dict['mode']
        if mode is None:
            return NO_RETENTION_FILE_SETTING

        return cls(
            RetentionMode(mode),
            file_retention_value_dict['retainUntilTimestamp'],
        )

    @classmethod
    def from_server_response(cls, server_response: dict) -> FileRetentionSetting:
        return cls.from_file_retention_value_dict(server_response['fileRetention'])

    @classmethod
    def from_response_headers(cls, headers) -> FileRetentionSetting:
        retention_mode_header = 'X-Bz-File-Retention-Mode'
        retain_until_header = 'X-Bz-File-Retention-Retain-Until-Timestamp'
        if retention_mode_header in headers:
            if retain_until_header in headers:
                retain_until = int(headers[retain_until_header])
            else:
                retain_until = None
            return cls(RetentionMode(headers[retention_mode_header]), retain_until)
        if 'X-Bz-Client-Unauthorized-To-Read' in headers and retention_mode_header in headers[
            'X-Bz-Client-Unauthorized-To-Read'].split(','):
            return UNKNOWN_FILE_RETENTION_SETTING
        return NO_RETENTION_FILE_SETTING  # the bucket is not file-lock-enabled or the file is has no retention set

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
        headers['X-Bz-File-Retention-Retain-Until-Timestamp'] = str(self.retain_until)

    def __eq__(self, other):
        return self.mode == other.mode and self.retain_until == other.retain_until

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.mode.value)}, {repr(self.retain_until)})'


@enum.unique
class LegalHold(enum.Enum):
    """Enum holding information about legalHold switch in a file."""

    ON = 'on'  #: legal hold set to "on"
    OFF = 'off'  #: legal hold set to "off"
    UNSET = None  #: server default, as for now it is functionally equivalent to OFF
    UNKNOWN = 'unknown'  #: the client is not authorized to read legal hold settings

    def is_on(self):
        """Is the legalHold switch on?"""
        return self is LegalHold.ON

    def is_off(self):
        """Is the legalHold switch off or left as default (which also means off)?"""
        return self is LegalHold.OFF or self is LegalHold.UNSET

    def is_unknown(self):
        """Is the legalHold switch unknown?"""
        return self is LegalHold.UNKNOWN

    @classmethod
    def from_file_version_dict(cls, file_version_dict: dict) -> LegalHold:
        if 'legalHold' not in file_version_dict:
            if file_version_dict['action'] not in ACTIONS_WITHOUT_LOCK_SETTINGS:
                raise UnexpectedCloudBehaviour(
                    'legalHold not provided for file version with action=%s' %
                    (file_version_dict['action'])
                )
            return cls.UNSET
        if not file_version_dict['legalHold']['isClientAuthorizedToRead']:
            return cls.UNKNOWN
        return cls.from_string_or_none(file_version_dict['legalHold']['value'])

    @classmethod
    def from_server_response(cls, server_response: dict) -> LegalHold:
        return cls.from_string_or_none(server_response['legalHold'])

    @classmethod
    def from_string_or_none(cls, string: str | None) -> LegalHold:
        return cls(string)

    @classmethod
    def from_response_headers(cls, headers) -> LegalHold:
        legal_hold_header = 'X-Bz-File-Legal-Hold'
        if legal_hold_header in headers:
            return cls(headers['X-Bz-File-Legal-Hold'])
        if 'X-Bz-Client-Unauthorized-To-Read' in headers and legal_hold_header in headers[
            'X-Bz-Client-Unauthorized-To-Read'].split(','):
            return cls.UNKNOWN
        return cls.UNSET  # the bucket is not file-lock-enabled or the header is missing for any other reason

    def to_server(self) -> str:
        if self.is_unknown():
            raise ValueError('Cannot use an unknown legal hold in requests')
        if self.is_on():
            return self.__class__.ON.value
        return self.__class__.OFF.value

    def add_to_upload_headers(self, headers):
        headers['X-Bz-File-Legal-Hold'] = self.to_server()


class BucketRetentionSetting:
    """Represent bucket's default file retention settings, i.e. whether the files should be retained, in which mode
       and for how long"""

    def __init__(self, mode: RetentionMode, period: RetentionPeriod | None = None):
        if mode in RETENTION_MODES_REQUIRING_PERIODS and period is None:
            raise ValueError(f'must specify period for retention mode {mode}')
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

    def __eq__(self, other):
        return self.mode == other.mode and self.period == other.period

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.mode.value)}, {repr(self.period)})'


class FileLockConfiguration:
    """Represent bucket's file lock configuration, i.e. whether the file lock mechanism is enabled and default
    file retention"""

    def __init__(
        self,
        default_retention: BucketRetentionSetting,
        is_file_lock_enabled: bool | None,
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

    def __eq__(self, other):
        return self.default_retention == other.default_retention and self.is_file_lock_enabled == other.is_file_lock_enabled

    def __repr__(self):
        return '{}({}, {})'.format(
            self.__class__.__name__, repr(self.default_retention), repr(self.is_file_lock_enabled)
        )


UNKNOWN_BUCKET_RETENTION = BucketRetentionSetting(RetentionMode.UNKNOWN)
"""Commonly used "unknown" default bucket retention setting"""
UNKNOWN_FILE_LOCK_CONFIGURATION = FileLockConfiguration(UNKNOWN_BUCKET_RETENTION, None)
"""Commonly used "unknown" bucket file lock setting"""
NO_RETENTION_BUCKET_SETTING = BucketRetentionSetting(RetentionMode.NONE)
"""Commonly used "no retention" default bucket retention"""
NO_RETENTION_FILE_SETTING = FileRetentionSetting(RetentionMode.NONE)
"""Commonly used "no retention" file setting"""
UNKNOWN_FILE_RETENTION_SETTING = FileRetentionSetting(RetentionMode.UNKNOWN)
"""Commonly used "unknown" file retention setting"""
