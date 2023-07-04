######################################################################
#
# File: test/unit/test_exception.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from apiver_deps_exception import (
    AlreadyFailed,
    B2Error,
    BadJson,
    BadUploadUrl,
    BucketIdNotFound,
    CapExceeded,
    Conflict,
    DuplicateBucketName,
    FileAlreadyHidden,
    FileNotPresent,
    InvalidAuthToken,
    MissingPart,
    PartSha1Mismatch,
    ServiceError,
    StorageCapExceeded,
    TooManyRequests,
    TransactionCapExceeded,
    Unauthorized,
    UnknownError,
    interpret_b2_error,
)

from b2sdk.exception import ResourceNotFound


class TestB2Error:
    def test_plain_ascii(self):
        assert 'message' == str(B2Error('message'))

    def test_unicode(self):
        assert '\u81ea\u7531' == str(B2Error('\u81ea\u7531'))


class TestExceptions:
    def test_bad_upload_url_exception(self):
        try:
            raise BadUploadUrl('foo')
        except BadUploadUrl as e:
            assert not e.should_retry_http()
            assert e.should_retry_upload()
            assert str(e) == 'Bad upload url: foo', str(e)

    def test_already_failed_exception(self):
        try:
            raise AlreadyFailed('foo')
        except AlreadyFailed as e:
            assert str(e) == 'Already failed: foo', str(e)

    @pytest.mark.apiver(to_ver=1)
    def test_command_error(self):
        from apiver_deps_exception import CommandError
        try:
            raise CommandError('foo')
        except CommandError as e:
            assert str(e) == 'foo', str(e)


class TestInterpretError:
    def test_file_already_hidden(self):
        self._check_one(FileAlreadyHidden, 400, 'already_hidden', '', {})
        assert 'File already hidden: file.txt' == \
            str(interpret_b2_error(400, 'already_hidden', '', {}, {'fileName': 'file.txt'}))

    def test_bad_json(self):
        self._check_one(BadJson, 400, 'bad_json', '', {})

    def test_file_not_present(self):
        self._check_one(FileNotPresent, 400, 'no_such_file', '', {})
        self._check_one(FileNotPresent, 400, 'file_not_present', '', {})
        self._check_one(FileNotPresent, 404, 'not_found', '', {})
        assert 'File not present: file.txt' == \
            str(interpret_b2_error(404, 'not_found', '', {}, {'fileName': 'file.txt'}))
        assert 'File not present: 01010101' == \
            str(interpret_b2_error(404, 'not_found', '', {}, {'fileId': '01010101'}))

    def test_file_or_bucket_not_present(self):
        self._check_one(ResourceNotFound, 404, None, None, {})
        assert 'No such file, bucket, or endpoint: ' == \
            str(interpret_b2_error(404, None, None, {}))

    def test_duplicate_bucket_name(self):
        self._check_one(DuplicateBucketName, 400, 'duplicate_bucket_name', '', {})
        assert 'Bucket name is already in use: my-bucket' == \
            str(
                interpret_b2_error(
                    400, 'duplicate_bucket_name', '', {}, {'bucketName': 'my-bucket'}
                )
            )

    def test_missing_part(self):
        self._check_one(MissingPart, 400, 'missing_part', '', {})
        assert 'Part number has not been uploaded: my-file-id' == \
            str(interpret_b2_error(400, 'missing_part', '', {}, {'fileId': 'my-file-id'}))

    def test_part_sha1_mismatch(self):
        self._check_one(PartSha1Mismatch, 400, 'part_sha1_mismatch', '', {})
        assert 'Part number my-file-id has wrong SHA1' == \
            str(interpret_b2_error(400, 'part_sha1_mismatch', '', {}, {'fileId': 'my-file-id'}))

    def test_unauthorized(self):
        self._check_one(Unauthorized, 401, '', '', {})

    def test_invalid_auth_token(self):
        self._check_one(InvalidAuthToken, 401, 'bad_auth_token', '', {})
        self._check_one(InvalidAuthToken, 401, 'expired_auth_token', '', {})

    def test_storage_cap_exceeded(self):
        self._check_one((CapExceeded, StorageCapExceeded), 403, 'storage_cap_exceeded', '', {})

    def test_transaction_cap_exceeded(self):
        self._check_one(
            (CapExceeded, TransactionCapExceeded), 403, 'transaction_cap_exceeded', '', {}
        )

    def test_conflict(self):
        self._check_one(Conflict, 409, '', '', {})

    def test_too_many_requests_with_retry_after_header(self):
        retry_after = 200
        error = self._check_one(
            TooManyRequests,
            429,
            '',
            '',
            {'retry-after': retry_after},
        )
        assert error.retry_after_seconds == retry_after

    def test_too_many_requests_without_retry_after_header(self):
        error = self._check_one(TooManyRequests, 429, '', '', {})
        assert error.retry_after_seconds is None

    @pytest.mark.apiver(
        from_ver=3
    )  # previous apivers throw this as well, but BucketIdNotFound is a different class in them
    def test_bad_bucket_id(self):
        error = self._check_one(
            BucketIdNotFound, 400, 'bad_bucket_id', '', {}, {'bucketId': '1001'}
        )
        assert error.bucket_id == '1001'

    def test_service_error(self):
        error = interpret_b2_error(500, 'code', 'message', {})
        assert isinstance(error, ServiceError)
        assert '500 code message' == str(error)

    def test_unknown_error(self):
        error = interpret_b2_error(499, 'code', 'message', {})
        assert isinstance(error, UnknownError)
        assert 'Unknown error: 499 code message' == str(error)

    @classmethod
    def _check_one(
        cls,
        expected_class,
        status,
        code,
        message,
        response_headers,
        post_params=None,
    ):
        actual_exception = interpret_b2_error(status, code, message, response_headers, post_params)
        assert isinstance(actual_exception, expected_class)
        return actual_exception
