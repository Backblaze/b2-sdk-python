######################################################################
#
# File: b2sdk/_internal/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import re
import typing
import warnings
from abc import ABCMeta
from typing import Any

from .utils import camelcase_to_underscore, trace_call

UPLOAD_TOKEN_USED_CONCURRENTLY_ERROR_MESSAGE_RE = re.compile(
    r'^more than one upload using auth token (?P<token>[^)]+)$'
)

COPY_SOURCE_TOO_BIG_ERROR_MESSAGE_RE = re.compile(r'^Copy source too big: (?P<size>[\d]+)$')

logger = logging.getLogger(__name__)


class B2Error(Exception, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        """
        Python 2 does not like it when you pass unicode as the message
        in an exception.  We like to use file names in exception messages.
        To avoid problems, if the message has any non-ascii characters in
        it, they are replaced with backslash-uNNNN.

        https://pythonhosted.org/kitchen/unicode-frustrations.html#frustration-5-exceptions
        """
        # If the exception is caused by a b2 server response,
        # the server MAY have included instructions to pause the thread before issuing any more requests
        self.retry_after_seconds = None
        super().__init__(*args, **kwargs)

    @property
    def prefix(self):
        """
        Nice, auto-generated error message prefix.

        >>> B2SimpleError().prefix
        'Simple error'
        >>> AlreadyFailed().prefix
        'Already failed'
        """
        prefix = self.__class__.__name__
        if prefix.startswith('B2'):
            prefix = prefix[2:]
        prefix = camelcase_to_underscore(prefix).replace('_', ' ')
        return prefix[0].upper() + prefix[1:]

    def should_retry_http(self):
        """
        Return true if this is an error that can cause an HTTP
        call to be retried.
        """
        return False

    def should_retry_upload(self):
        """
        Return true if this is an error that should tell the upload
        code to get a new upload URL and try the upload again.
        """
        return False


class InvalidUserInput(B2Error):
    pass


class B2SimpleError(B2Error, metaclass=ABCMeta):
    """
    A B2Error with a message prefix.
    """

    def __str__(self):
        return f'{self.prefix}: {super().__str__()}'


class NotAllowedByAppKeyError(B2SimpleError, metaclass=ABCMeta):
    """
    Base class for errors caused by restrictions on an application key.
    """


class TransientErrorMixin(metaclass=ABCMeta):
    def should_retry_http(self):
        return True

    def should_retry_upload(self):
        return True


class AlreadyFailed(B2SimpleError):
    pass


class BadDateFormat(B2SimpleError):
    prefix = 'Date from server'


class BadFileInfo(B2SimpleError):
    pass


class BadJson(B2SimpleError):
    prefix = 'Bad request'


class BadUploadUrl(B2SimpleError):
    def should_retry_upload(self):
        return True


class BrokenPipe(B2Error):
    def __str__(self):
        return 'Broken pipe: unable to send entire request'

    def should_retry_upload(self):
        return True


class CapabilityNotAllowed(NotAllowedByAppKeyError):
    pass


class ChecksumMismatch(TransientErrorMixin, B2Error):
    def __init__(self, checksum_type, expected, actual):
        super().__init__()
        self.checksum_type = checksum_type
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return f'{self.checksum_type} checksum mismatch -- bad data'


class B2HttpCallbackException(B2SimpleError):
    pass


class B2HttpCallbackPostRequestException(B2HttpCallbackException):
    pass


class B2HttpCallbackPreRequestException(B2HttpCallbackException):
    pass


class BucketNotAllowed(NotAllowedByAppKeyError):
    pass


class ClockSkew(B2HttpCallbackPostRequestException):
    """
    The clock on the server differs from the local clock by too much.
    """

    def __init__(self, clock_skew_seconds):
        """
        :param int clock_skew_seconds: The difference: local_clock - server_clock
        """
        super().__init__()
        self.clock_skew_seconds = clock_skew_seconds

    def __str__(self):
        if self.clock_skew_seconds < 0:
            return 'ClockSkew: local clock is %d seconds behind server' % (
                -self.clock_skew_seconds,
            )
        else:
            return 'ClockSkew; local clock is %d seconds ahead of server' % (
                self.clock_skew_seconds,
            )


class Conflict(B2SimpleError):
    pass


class ConnectionReset(B2Error):
    def __str__(self):
        return 'Connection reset'

    def should_retry_upload(self):
        return True


class B2ConnectionError(TransientErrorMixin, B2SimpleError):
    pass


class B2RequestTimeout(TransientErrorMixin, B2SimpleError):
    pass


class B2RequestTimeoutDuringUpload(B2RequestTimeout):
    # if a timeout is hit during upload, it is not guaranteed that the the server has released the upload token lock already, so we'll use a new token
    def should_retry_http(self):
        return False


class DestFileNewer(B2Error):
    def __init__(self, dest_path, source_path, dest_prefix, source_prefix):
        super().__init__()
        self.dest_path = dest_path
        self.source_path = source_path
        self.dest_prefix = dest_prefix
        self.source_prefix = source_prefix

    def __str__(self):
        return 'source file is older than destination: {}{} with a time of {} cannot be synced to {}{} with a time of {}, unless a valid newer_file_mode is provided'.format(
            self.source_prefix,
            self.source_path.relative_path,
            self.source_path.mod_time,
            self.dest_prefix,
            self.dest_path.relative_path,
            self.dest_path.mod_time,
        )

    def should_retry_http(self):
        return True


class DuplicateBucketName(B2SimpleError):
    prefix = 'Bucket name is already in use'


class ResourceNotFound(B2SimpleError):
    prefix = 'No such file, bucket, or endpoint'


class FileOrBucketNotFound(ResourceNotFound):
    def __init__(self, bucket_name=None, file_id_or_name=None):
        super().__init__()
        self.bucket_name = bucket_name
        self.file_id_or_name = file_id_or_name

    def __str__(self):
        file_str = ('file [%s]' % self.file_id_or_name) if self.file_id_or_name else 'a file'
        bucket_str = ('bucket [%s]' % self.bucket_name) if self.bucket_name else 'a bucket'
        return f'Could not find {file_str} within {bucket_str}'


class BucketIdNotFound(ResourceNotFound):
    def __init__(self, bucket_id):
        self.bucket_id = bucket_id

    def __str__(self):
        return f'Bucket with id={self.bucket_id} not found'


class FileAlreadyHidden(B2SimpleError):
    pass


class FileNotHidden(B2SimpleError):
    prefix = 'File not hidden'


class FileDeleted(B2SimpleError):
    prefix = 'File deleted'


class UnexpectedFileVersionAction(B2SimpleError):
    prefix = 'Unexpected file version action returned by the server'


class FileNameNotAllowed(NotAllowedByAppKeyError):
    pass


class FileNotPresent(FileOrBucketNotFound):
    def __str__(self):  # overridden to retain message across prev versions
        return "File not present%s" % (': ' + self.file_id_or_name if self.file_id_or_name else "")


class UnusableFileName(B2SimpleError):
    """
    Raise when a filename doesn't meet the rules.

    Could possibly use InvalidUploadSource, but this is intended for the filename on the
    server, which could differ.  https://www.backblaze.com/b2/docs/files.html.
    """
    pass


class InvalidMetadataDirective(B2Error):
    pass


class SSECKeyIdMismatchInCopy(InvalidMetadataDirective):
    pass


class InvalidRange(B2Error):
    def __init__(self, content_length, range_):
        super().__init__()
        self.content_length = content_length
        self.range_ = range_

    def __str__(self):
        return 'A range of %d-%d was requested (size of %d), but cloud could only serve %d of that' % (
            self.range_[0],
            self.range_[1],
            self.range_[1] - self.range_[0] + 1,
            self.content_length,
        )


class InvalidUploadSource(B2SimpleError):
    pass


class BadRequest(B2Error):
    def __init__(self, message, code):
        super().__init__()
        self.message = message
        self.code = code

    def __str__(self):
        return f'{self.message} ({self.code})'


class CopySourceTooBig(BadRequest):
    def __init__(self, message, code, size: int):
        super().__init__(message, code)
        self.size = size


class Unauthorized(B2Error):
    def __init__(self, message, code):
        super().__init__()
        self.message = message
        self.code = code

    def __str__(self):
        return f'{self.message} ({self.code})'

    def should_retry_upload(self):
        return True


class EmailNotVerified(Unauthorized):
    def should_retry_upload(self):
        return False


class NoPaymentHistory(Unauthorized):
    def should_retry_upload(self):
        return False


class InvalidAuthToken(Unauthorized):
    """
    Specific type of Unauthorized that means the auth token is invalid.
    This is not the case where the auth token is valid, but does not
    allow access.
    """

    def __init__(self, message, code):
        super().__init__('Invalid authorization token. Server said: ' + message, code)


class RestrictedBucket(B2Error):
    def __init__(self, bucket_name):
        super().__init__()
        self.bucket_name = bucket_name

    def __str__(self):
        return 'Application key is restricted to bucket: %s' % self.bucket_name


class RestrictedBucketMissing(RestrictedBucket):
    def __init__(self):
        super().__init__('')

    def __str__(self):
        return 'Application key is restricted to a bucket that doesn\'t exist'


class MaxFileSizeExceeded(B2Error):
    def __init__(self, size, max_allowed_size):
        super().__init__()
        self.size = size
        self.max_allowed_size = max_allowed_size

    def __str__(self):
        return f'Allowed file size of exceeded: {self.size} > {self.max_allowed_size}'


class MaxRetriesExceeded(B2Error):
    def __init__(self, limit, exception_info_list):
        super().__init__()
        self.limit = limit
        self.exception_info_list = exception_info_list

    def __str__(self):
        exceptions = '\n'.join(str(wrapped_error) for wrapped_error in self.exception_info_list)
        return f'FAILED to upload after {self.limit} tries. Encountered exceptions: {exceptions}'


class MissingPart(B2SimpleError):
    prefix = 'Part number has not been uploaded'


class NonExistentBucket(FileOrBucketNotFound):
    def __str__(self):  # overridden to retain message across prev versions
        return "No such bucket%s" % (': ' + self.bucket_name if self.bucket_name else "")


class FileSha1Mismatch(B2SimpleError):
    prefix = 'Upload file SHA1 mismatch'


class PartSha1Mismatch(B2Error):
    def __init__(self, key):
        super().__init__()
        self.key = key

    def __str__(self):
        return f'Part number {self.key} has wrong SHA1'


class ServiceError(TransientErrorMixin, B2Error):
    """
    Used for HTTP status codes 500 through 599.
    """


class CapExceeded(B2Error):
    def __str__(self):
        return 'Cap exceeded.'


class StorageCapExceeded(CapExceeded):
    def __str__(self):
        return 'Cannot upload or copy files, storage cap exceeded.'


class TransactionCapExceeded(CapExceeded):
    def __str__(self):
        return 'Cannot perform the operation, transaction cap exceeded.'


class TooManyRequests(B2Error):
    def __init__(self, retry_after_seconds=None):
        super().__init__()
        self.retry_after_seconds = retry_after_seconds

    def __str__(self):
        return 'Too many requests'

    def should_retry_http(self):
        return True


class TruncatedOutput(TransientErrorMixin, B2Error):
    def __init__(self, bytes_read, file_size):
        super().__init__()
        self.bytes_read = bytes_read
        self.file_size = file_size

    def __str__(self):
        return 'only %d of %d bytes read' % (
            self.bytes_read,
            self.file_size,
        )


class UnexpectedCloudBehaviour(B2SimpleError):
    pass


class UnknownError(B2SimpleError):
    pass


class UnknownHost(B2Error):
    def __str__(self):
        return 'unknown host'


class UnrecognizedBucketType(B2Error):
    pass


class UnsatisfiableRange(B2Error):
    def __str__(self):
        return "The range in the request is outside the size of the file"


class UploadTokenUsedConcurrently(B2Error):
    def __init__(self, token):
        super().__init__()
        self.token = token

    def __str__(self):
        return f"More than one concurrent upload using auth token {self.token}"


class AccessDenied(B2Error):
    def __str__(self):
        return "This call with these parameters is not allowed for this auth token"


class SSECKeyError(AccessDenied):
    def __str__(self):
        return "Wrong or no SSE-C key provided when reading a file."


class RetentionWriteError(AccessDenied):
    def __str__(self):
        return "Auth token not authorized to write retention or file already in 'compliance' mode or " \
               "bypassGovernance=true parameter missing"


class WrongEncryptionModeForBucketDefault(InvalidUserInput):
    def __init__(self, encryption_mode):
        super().__init__()
        self.encryption_mode = encryption_mode

    def __str__(self):
        return f"{self.encryption_mode} cannot be used as default for a bucket."


class CopyArgumentsMismatch(InvalidUserInput):
    pass


class DisablingFileLockNotSupported(B2Error):
    def __str__(self):
        return "Disabling file lock is not supported"


class SourceReplicationConflict(B2Error):
    def __str__(self):
        return "Operation not supported for buckets with source replication"


class EnablingFileLockOnRestrictedBucket(B2Error):
    def __str__(self):
        return "Turning on file lock for a restricted bucket is not allowed"


class InvalidJsonResponse(B2SimpleError):
    UP_TO_BYTES_COUNT = 200

    def __init__(self, content: bytes):
        self.content = content
        message = self.content[:self.UP_TO_BYTES_COUNT].decode('utf-8', errors='replace')
        if len(self.content) > self.UP_TO_BYTES_COUNT:
            message += '...'

        super().__init__(message)


class PotentialS3EndpointPassedAsRealm(InvalidJsonResponse):
    pass


class DestinationError(B2Error):
    pass


class DestinationDirectoryError(DestinationError):
    pass


class DestinationDirectoryDoesntExist(DestinationDirectoryError):
    pass


class DestinationParentIsNotADirectory(DestinationDirectoryError):
    pass


class DestinationIsADirectory(DestinationDirectoryError):
    pass


class DestinationDirectoryDoesntAllowOperation(DestinationDirectoryError):
    pass


class EventTypeError(BadRequest):
    pass


class EventTypeCategoriesError(EventTypeError):
    pass


class EventTypeOverlapError(EventTypeError):
    pass


class EventTypesEmptyError(EventTypeError):
    pass


class EventTypeInvalidError(EventTypeError):
    pass


def _event_type_invalid_error(code: str, message: str, **_) -> B2Error:
    from b2sdk._internal.raw_api import EVENT_TYPE

    valid_types = sorted(typing.get_args(EVENT_TYPE))
    return EventTypeInvalidError(
        f"Event Type error: {message!r}. Valid types: {sorted(valid_types)!r}", code
    )


_error_handlers: dict[tuple[int, str | None], typing.Callable] = {
    (400, "event_type_categories"):
        lambda code, message, **_: EventTypeCategoriesError(message, code),
    (400, "event_type_overlap"):
        lambda code, message, **_: EventTypeOverlapError(message, code),
    (400, "event_types_empty"):
        lambda code, message, **_: EventTypesEmptyError(message, code),
    (400, "event_type_invalid"):
        _event_type_invalid_error,
    (401, "email_not_verified"):
        lambda code, message, **_: EmailNotVerified(message, code),
    (401, "no_payment_history"):
        lambda code, message, **_: NoPaymentHistory(message, code),
}


@trace_call(logger)
def interpret_b2_error(
    status: int,
    code: str | None,
    message: str | None,
    response_headers: dict[str, Any],
    post_params: dict[str, Any] | None = None
) -> B2Error:
    post_params = post_params or {}

    handler = _error_handlers.get((status, code))
    if handler:
        error = handler(
            status=status,
            code=code,
            message=message,
            response_headers=response_headers,
            post_params=post_params
        )
        if error:
            return error

    if status == 400 and code == "already_hidden":
        return FileAlreadyHidden(post_params.get('fileName'))
    elif status == 400 and code == 'bad_json':
        return BadJson(message)
    elif (
        (status == 400 and code in ("no_such_file", "file_not_present")) or
        (status == 404 and code == "not_found")
    ):
        # hide_file returns 400 and "no_such_file"
        # delete_file_version returns 400 and "file_not_present"
        # get_file_info returns 404 and "not_found"
        # download_file_by_name/download_file_by_id return 404 and "not_found"
        # but don't have post_params
        return FileNotPresent(
            file_id_or_name=post_params.get('fileId') or post_params.get('fileName')
        )
    elif status == 404:
        # often times backblaze will return cryptic error messages on invalid URLs.
        # We should ideally only reach that case on programming error or outdated
        # sdk versions, but to prevent user confusion we omit the message param
        return ResourceNotFound()
    elif status == 400 and code == "duplicate_bucket_name":
        return DuplicateBucketName(post_params.get('bucketName'))
    elif status == 400 and code == "missing_part":
        return MissingPart(post_params.get('fileId'))
    elif status == 400 and code == "part_sha1_mismatch":
        return PartSha1Mismatch(post_params.get('fileId'))
    elif status == 400 and code == "bad_bucket_id":
        return BucketIdNotFound(post_params.get('bucketId'))
    elif status == 400 and code == "auth_token_limit":
        matcher = UPLOAD_TOKEN_USED_CONCURRENTLY_ERROR_MESSAGE_RE.match(message)
        assert matcher is not None, f"unexpected error message: {message}"
        token = matcher.group('token')
        return UploadTokenUsedConcurrently(token)
    elif status == 400 and code == "source_too_large":
        matcher = COPY_SOURCE_TOO_BIG_ERROR_MESSAGE_RE.match(message)
        assert matcher is not None, f"unexpected error message: {message}"
        size = int(matcher.group('size'))
        return CopySourceTooBig(message, code, size)
    elif status == 400 and code == 'file_lock_conflict':
        return DisablingFileLockNotSupported()
    elif status == 400 and code == 'source_replication_conflict':
        return SourceReplicationConflict()
    elif status == 400 and code == 'restricted_bucket_conflict':
        return EnablingFileLockOnRestrictedBucket()
    elif status == 400 and code == 'bad_request':

        # it's "bad_request" on 2022-09-14, but will become 'disabling_file_lock_not_allowed'  # TODO: cleanup after 2022-09-22
        if message == 'fileLockEnabled value of false is not allowed when bucket is already file lock enabled.':
            return DisablingFileLockNotSupported()

        # it's "bad_request" on 2022-09-14, but will become 'source_replication_conflict'  # TODO: cleanup after 2022-09-22
        if message == 'Turning on file lock for an existing bucket having source replication configuration is not allowed.':
            return SourceReplicationConflict()

        # it's "bad_request" on 2022-09-14, but will become 'restricted_bucket_conflict'  # TODO: cleanup after 2022-09-22
        if message == 'Turning on file lock for a restricted bucket is not allowed.':
            return EnablingFileLockOnRestrictedBucket()

        return BadRequest(message, code)
    elif status == 400:
        warnings.warn(
            f"bad request exception with an unknown `code`. message={message}, code={code}"
        )
        return BadRequest(message, code)
    elif status == 401 and code in ("bad_auth_token", "expired_auth_token"):
        return InvalidAuthToken(message, code)
    elif status == 401:
        return Unauthorized(message, code)
    elif status == 403 and code == "storage_cap_exceeded":
        return StorageCapExceeded()
    elif status == 403 and code == "transaction_cap_exceeded":
        return TransactionCapExceeded()
    elif status == 403 and code == "access_denied":
        return AccessDenied()
    elif status == 409:
        return Conflict()
    elif status == 416 and code == "range_not_satisfiable":
        return UnsatisfiableRange()
    elif status == 429:
        return TooManyRequests(retry_after_seconds=response_headers.get('retry-after'))
    elif 500 <= status < 600:
        return ServiceError('%d %s %s' % (status, code, message))
    return UnknownError('%d %s %s' % (status, code, message))
