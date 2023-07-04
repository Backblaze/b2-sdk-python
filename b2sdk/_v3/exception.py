######################################################################
#
# File: b2sdk/_v3/exception.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk.account_info.exception import AccountInfoError
from b2sdk.account_info.exception import CorruptAccountInfo
from b2sdk.account_info.exception import MissingAccountData
from b2sdk.exception import AccessDenied
from b2sdk.exception import AlreadyFailed
from b2sdk.exception import B2ConnectionError
from b2sdk.exception import B2Error
from b2sdk.exception import B2HttpCallbackException
from b2sdk.exception import B2HttpCallbackPostRequestException
from b2sdk.exception import B2HttpCallbackPreRequestException
from b2sdk.exception import B2RequestTimeout
from b2sdk.exception import B2RequestTimeoutDuringUpload
from b2sdk.exception import B2SimpleError
from b2sdk.exception import BadDateFormat
from b2sdk.exception import BadFileInfo
from b2sdk.exception import BadJson
from b2sdk.exception import BadRequest
from b2sdk.exception import BadUploadUrl
from b2sdk.exception import BrokenPipe
from b2sdk.exception import BucketIdNotFound
from b2sdk.exception import BucketNotAllowed
from b2sdk.exception import CapExceeded
from b2sdk.exception import CapabilityNotAllowed
from b2sdk.exception import ChecksumMismatch
from b2sdk.exception import ClockSkew
from b2sdk.exception import Conflict
from b2sdk.exception import ConnectionReset
from b2sdk.exception import CopyArgumentsMismatch
from b2sdk.exception import DestFileNewer
from b2sdk.exception import DestinationDirectoryDoesntAllowOperation
from b2sdk.exception import DestinationDirectoryDoesntExist
from b2sdk.exception import DestinationIsADirectory
from b2sdk.exception import DestinationParentIsNotADirectory
from b2sdk.exception import DisablingFileLockNotSupported
from b2sdk.exception import DuplicateBucketName
from b2sdk.exception import FileAlreadyHidden
from b2sdk.exception import FileNameNotAllowed
from b2sdk.exception import FileNotPresent
from b2sdk.exception import FileSha1Mismatch
from b2sdk.exception import InvalidAuthToken
from b2sdk.exception import InvalidJsonResponse
from b2sdk.exception import InvalidMetadataDirective
from b2sdk.exception import InvalidRange
from b2sdk.exception import InvalidUploadSource
from b2sdk.exception import MaxFileSizeExceeded
from b2sdk.exception import MaxRetriesExceeded
from b2sdk.exception import MissingPart
from b2sdk.exception import NonExistentBucket
from b2sdk.exception import NotAllowedByAppKeyError
from b2sdk.exception import PartSha1Mismatch
from b2sdk.exception import PotentialS3EndpointPassedAsRealm
from b2sdk.exception import RestrictedBucket
from b2sdk.exception import RestrictedBucketMissing
from b2sdk.exception import RetentionWriteError
from b2sdk.exception import SSECKeyError
from b2sdk.exception import SSECKeyIdMismatchInCopy
from b2sdk.exception import ServiceError
from b2sdk.exception import SourceReplicationConflict
from b2sdk.exception import StorageCapExceeded
from b2sdk.exception import TooManyRequests
from b2sdk.exception import TransactionCapExceeded
from b2sdk.exception import TransientErrorMixin
from b2sdk.exception import TruncatedOutput
from b2sdk.exception import Unauthorized
from b2sdk.exception import UnexpectedCloudBehaviour
from b2sdk.exception import UnknownError
from b2sdk.exception import UnknownHost
from b2sdk.exception import UnrecognizedBucketType
from b2sdk.exception import UnsatisfiableRange
from b2sdk.exception import UnusableFileName
from b2sdk.exception import WrongEncryptionModeForBucketDefault
from b2sdk.exception import interpret_b2_error
from b2sdk.sync.exception import IncompleteSync
from b2sdk.scan.exception import UnableToCreateDirectory
from b2sdk.scan.exception import EmptyDirectory
from b2sdk.scan.exception import EnvironmentEncodingError
from b2sdk.scan.exception import InvalidArgument
from b2sdk.scan.exception import NotADirectory
from b2sdk.scan.exception import UnsupportedFilename
from b2sdk.scan.exception import check_invalid_argument

__all__ = (
    'AccessDenied',
    'AccountInfoError',
    'AlreadyFailed',
    'B2ConnectionError',
    'B2Error',
    'B2HttpCallbackException',
    'B2HttpCallbackPostRequestException',
    'B2HttpCallbackPreRequestException',
    'B2RequestTimeout',
    'B2RequestTimeoutDuringUpload',
    'B2SimpleError',
    'BadDateFormat',
    'BadFileInfo',
    'BadJson',
    'BadRequest',
    'BadUploadUrl',
    'BrokenPipe',
    'BucketIdNotFound',
    'BucketNotAllowed',
    'CapExceeded',
    'CapabilityNotAllowed',
    'ChecksumMismatch',
    'ClockSkew',
    'Conflict',
    'ConnectionReset',
    'CopyArgumentsMismatch',
    'CorruptAccountInfo',
    'DestFileNewer',
    'DestinationDirectoryDoesntAllowOperation',
    'DestinationDirectoryDoesntExist',
    'DestinationIsADirectory',
    'DestinationParentIsNotADirectory',
    'DisablingFileLockNotSupported',
    'DuplicateBucketName',
    'EmptyDirectory',
    'EnvironmentEncodingError',
    'FileAlreadyHidden',
    'FileNameNotAllowed',
    'FileNotPresent',
    'FileSha1Mismatch',
    'IncompleteSync',
    'InvalidArgument',
    'InvalidAuthToken',
    'InvalidJsonResponse',
    'InvalidMetadataDirective',
    'InvalidRange',
    'InvalidUploadSource',
    'MaxFileSizeExceeded',
    'MaxRetriesExceeded',
    'MissingAccountData',
    'MissingPart',
    'NonExistentBucket',
    'NotADirectory',
    'NotAllowedByAppKeyError',
    'PartSha1Mismatch',
    'PotentialS3EndpointPassedAsRealm',
    'RestrictedBucket',
    'RestrictedBucketMissing',
    'RetentionWriteError',
    'ServiceError',
    'SourceReplicationConflict',
    'StorageCapExceeded',
    'TooManyRequests',
    'TransactionCapExceeded',
    'TransientErrorMixin',
    'TruncatedOutput',
    'UnableToCreateDirectory',
    'Unauthorized',
    'UnexpectedCloudBehaviour',
    'UnknownError',
    'UnknownHost',
    'UnrecognizedBucketType',
    'UnsatisfiableRange',
    'UnsupportedFilename',
    'UnusableFileName',
    'interpret_b2_error',
    'check_invalid_argument',
    'SSECKeyIdMismatchInCopy',
    'SSECKeyError',
    'WrongEncryptionModeForBucketDefault',
)
