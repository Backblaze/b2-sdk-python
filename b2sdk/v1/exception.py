from b2sdk.account_info.exception import AccountInfoError
from b2sdk.account_info.exception import CorruptAccountInfo
from b2sdk.account_info.exception import MissingAccountData
from b2sdk.exception import AlreadyFailed
from b2sdk.exception import B2ConnectionError
from b2sdk.exception import B2Error
from b2sdk.exception import B2HttpCallbackException
from b2sdk.exception import B2HttpCallbackPostRequestException
from b2sdk.exception import B2HttpCallbackPreRequestException
from b2sdk.exception import B2RequestTimeout
from b2sdk.exception import B2SimpleError
from b2sdk.exception import BadDateFormat
from b2sdk.exception import BadFileInfo
from b2sdk.exception import BadJson
from b2sdk.exception import BadUploadUrl
from b2sdk.exception import BrokenPipe
from b2sdk.exception import BucketNotAllowed
from b2sdk.exception import CapabilityNotAllowed
from b2sdk.exception import ChecksumMismatch
from b2sdk.exception import ClockSkew
from b2sdk.exception import CommandError
from b2sdk.exception import Conflict
from b2sdk.exception import ConnectionReset
from b2sdk.exception import DestFileNewer
from b2sdk.exception import DuplicateBucketName
from b2sdk.exception import FileAlreadyHidden
from b2sdk.exception import FileNameNotAllowed
from b2sdk.exception import FileNotPresent
from b2sdk.exception import InvalidAuthToken
from b2sdk.exception import InvalidRange
from b2sdk.exception import InvalidUploadSource
from b2sdk.exception import MaxFileSizeExceeded
from b2sdk.exception import MaxRetriesExceeded
from b2sdk.exception import MissingPart
from b2sdk.exception import NonExistentBucket
from b2sdk.exception import NotAllowedByAppKeyError
from b2sdk.exception import PartSha1Mismatch
from b2sdk.exception import RestrictedBucket
from b2sdk.exception import ServiceError
from b2sdk.exception import StorageCapExceeded
from b2sdk.exception import TooManyRequests
from b2sdk.exception import TransientErrorMixin
from b2sdk.exception import TruncatedOutput
from b2sdk.exception import Unauthorized
from b2sdk.exception import UnexpectedCloudBehaviour
from b2sdk.exception import UnknownError
from b2sdk.exception import UnknownHost
from b2sdk.exception import UnrecognizedBucketType
from b2sdk.exception import UnusableFileName
from b2sdk.sync.exception import EnvironmentEncodingError

assert AccountInfoError
assert CorruptAccountInfo
assert MissingAccountData
assert AlreadyFailed
assert B2ConnectionError
assert B2Error
assert B2HttpCallbackException
assert B2HttpCallbackPostRequestException
assert B2HttpCallbackPreRequestException
assert B2RequestTimeout
assert B2SimpleError
assert BadDateFormat
assert BadFileInfo
assert BadJson
assert BadUploadUrl
assert BrokenPipe
assert BucketNotAllowed
assert CapabilityNotAllowed
assert ChecksumMismatch
assert ClockSkew
assert CommandError
assert Conflict
assert ConnectionReset
assert DestFileNewer
assert DuplicateBucketName
assert FileAlreadyHidden
assert FileNameNotAllowed
assert FileNotPresent
assert InvalidAuthToken
assert InvalidRange
assert InvalidUploadSource
assert MaxFileSizeExceeded
assert MaxRetriesExceeded
assert MissingPart
assert NonExistentBucket
assert NotAllowedByAppKeyError
assert PartSha1Mismatch
assert RestrictedBucket
assert ServiceError
assert StorageCapExceeded
assert TooManyRequests
assert TransientErrorMixin
assert TruncatedOutput
assert Unauthorized
assert UnexpectedCloudBehaviour
assert UnknownError
assert UnknownHost
assert UnrecognizedBucketType
assert UnusableFileName
assert EnvironmentEncodingError
