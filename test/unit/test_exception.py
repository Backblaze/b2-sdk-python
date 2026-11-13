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

import base64
import json
import logging
import re
import secrets
from contextlib import closing
from logging.handlers import BufferingHandler

import pytest
import responses
from apiver_deps import (
    B2RawHTTPApi,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
)
from apiver_deps_exception import (
    AlreadyFailed,
    B2Error,
    BadJson,
    BadUploadUrl,
    BucketIdNotFound,
    CapExceeded,
    Conflict,
    DuplicateBucketName,
    EmailNotVerified,
    FileAlreadyHidden,
    FileNotPresent,
    InvalidAuthToken,
    MissingPart,
    NoPaymentHistory,
    PartSha1Mismatch,
    ServiceError,
    StorageCapExceeded,
    TooManyRequests,
    TransactionCapExceeded,
    Unauthorized,
    UnknownError,
    interpret_b2_error,
)

from b2sdk._internal.api_config import B2HttpApiConfig
from b2sdk._internal.b2http import B2Http
from b2sdk._internal.exception import BadRequest, RequestTimeout, ResourceNotFound, SSECKeyError


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
        assert 'File already hidden: file.txt' == str(
            interpret_b2_error(400, 'already_hidden', '', {}, {'fileName': 'file.txt'})
        )

    def test_bad_json(self):
        self._check_one(BadJson, 400, 'bad_json', '', {})

    def test_file_not_present(self):
        self._check_one(FileNotPresent, 400, 'no_such_file', '', {})
        self._check_one(FileNotPresent, 400, 'file_not_present', '', {})
        self._check_one(FileNotPresent, 404, 'not_found', '', {})
        assert 'File not present: file.txt' == str(
            interpret_b2_error(404, 'not_found', '', {}, {'fileName': 'file.txt'})
        )
        assert 'File not present: 01010101' == str(
            interpret_b2_error(404, 'not_found', '', {}, {'fileId': '01010101'})
        )

    def test_file_or_bucket_not_present(self):
        self._check_one(ResourceNotFound, 404, None, None, {})
        assert 'No such file, bucket, or endpoint: ' == str(interpret_b2_error(404, None, None, {}))

    def test_duplicate_bucket_name(self):
        self._check_one(DuplicateBucketName, 400, 'duplicate_bucket_name', '', {})
        assert 'Bucket name is already in use: my-bucket' == str(
            interpret_b2_error(400, 'duplicate_bucket_name', '', {}, {'bucketName': 'my-bucket'})
        )

    def test_missing_part(self):
        self._check_one(MissingPart, 400, 'missing_part', '', {})
        assert 'Part number has not been uploaded: my-file-id' == str(
            interpret_b2_error(400, 'missing_part', '', {}, {'fileId': 'my-file-id'})
        )

    def test_part_sha1_mismatch(self):
        self._check_one(PartSha1Mismatch, 400, 'part_sha1_mismatch', '', {})
        assert 'Part number my-file-id has wrong SHA1' == str(
            interpret_b2_error(400, 'part_sha1_mismatch', '', {}, {'fileId': 'my-file-id'})
        )

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

    def test_service_error_with_retry_after_header(self):
        retry_after = 200
        error = self._check_one(ServiceError, 503, '', '', {'retry-after': retry_after})
        assert error.retry_after_seconds == retry_after

    def test_service_error_without_retry_after_header(self):
        error = self._check_one(ServiceError, 503, '', '', {})
        assert error.retry_after_seconds is None

    @pytest.mark.parametrize('use_keywords', [False, True], ids=['positional', 'keyword'])
    def test_trace_uses_only_safe_arguments(self, caplog, monkeypatch, use_keywords):
        key_value = base64.b64encode(secrets.token_bytes(32)).decode('ascii')
        header_value = secrets.token_urlsafe(24)
        message_value = secrets.token_urlsafe(24)
        arguments = dict(
            status=503,
            code='server_busy',
            message=message_value,
            response_headers={'retry-after': '7', 'X-Trace-Test': header_value},
            post_params={'serverSideEncryption': {'mode': 'SSE-C', 'customerKey': key_value}},
        )
        logger = logging.getLogger('b2sdk._internal.exception')
        with closing(BufferingHandler(capacity=100)) as handler, monkeypatch.context() as patch:
            # Keep test values in memory, including on a failed assertion.
            patch.setattr(logger, 'handlers', [handler])
            patch.setattr(logger, 'propagate', False)
            with caplog.at_level(logging.DEBUG, logger=logger.name):
                if use_keywords:
                    error = interpret_b2_error(**arguments)
                else:
                    error = interpret_b2_error(*arguments.values())

            messages = [record.getMessage() for record in handler.buffer]
            has_private_values = any(
                value in message
                for value in (key_value, header_value, message_value)
                for message in messages
            )
            assert not has_private_values, 'trace included a disallowed argument value'
            assert any(
                'status=503' in message and "code='server_busy'" in message for message in messages
            )

        assert isinstance(error, ServiceError)
        assert error.retry_after_seconds == 7

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

    def test_request_timeout(self):
        error = interpret_b2_error(408, 'request_timeout', 'request timeout', {})
        assert isinstance(error, RequestTimeout)
        assert error.should_retry_upload()
        assert error.should_retry_http()

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

    @pytest.mark.parametrize(
        'status, code, expected_exception_cls',
        [
            (401, 'email_not_verified', EmailNotVerified),
            (401, 'no_payment_history', NoPaymentHistory),
        ],
    )
    def test_simple_error_handlers(self, status, code, expected_exception_cls):
        error = interpret_b2_error(status, code, '', {})
        assert isinstance(error, expected_exception_cls)
        assert error.code == code


@pytest.mark.parametrize('operation', ['notification_rules', 'sse_c_copy'])
def test_http_error_trace_keeps_request_values_private(bucket, caplog, monkeypatch, operation):
    signing_value = secrets.token_hex(16)
    header_value = secrets.token_urlsafe(24)
    response_value = secrets.token_urlsafe(24)
    message_value = secrets.token_urlsafe(24)
    key = EncryptionKey(secret=secrets.token_bytes(32), key_id=None)
    key_value = key.key_b64()
    encryption = EncryptionSetting(EncryptionMode.SSE_C, EncryptionAlgorithm.AES256, key)
    rules = [
        {
            'eventTypes': ['b2:ObjectCreated:*'],
            'isEnabled': True,
            'name': 'test-rule',
            'objectNamePrefix': '',
            'targetConfiguration': {
                'targetType': 'webhook',
                'url': 'https://example.com/webhook',
                'hmacSha256SigningSecret': signing_value,
                'customHeaders': [{'name': 'X-Test-Header', 'value': header_value}],
            },
        }
    ]
    if operation == 'notification_rules':
        endpoint, status, code, error_class = (
            'b2_set_bucket_notification_rules',
            400,
            'bad_request',
            BadRequest,
        )
    else:
        endpoint, status, code, error_class = 'b2_copy_file', 403, 'access_denied', SSECKeyError

    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    bucket.api.session.raw_api = B2RawHTTPApi(http)
    logger = logging.getLogger('b2sdk')
    with (
        closing(BufferingHandler(capacity=100)) as handler,
        monkeypatch.context() as patch,
        responses.RequestsMock() as transport,
    ):
        # Capture the entire SDK namespace without letting values reach pytest's log output.
        patch.setattr(logger, 'handlers', [handler])
        patch.setattr(logger, 'propagate', False)
        transport.add(
            responses.POST,
            re.compile(r'.*/' + endpoint + '$'),
            status=status,
            json={'status': status, 'code': code, 'message': message_value},
            headers={'X-Test-Response': response_value},
        )
        with caplog.at_level(logging.DEBUG, logger=logger.name), pytest.raises(error_class):
            if operation == 'notification_rules':
                bucket.set_notification_rules(rules)
            else:
                bucket.api.session.copy_file(
                    'source-file', 'target-file', source_server_side_encryption=encryption
                )

        assert len(transport.calls) == 1
        request = json.loads(transport.calls[0].request.body)
        if operation == 'notification_rules':
            target = request['eventNotificationRules'][0]['targetConfiguration']
            request_has_values = (
                target['hmacSha256SigningSecret'] == signing_value
                and target['customHeaders'][0]['value'] == header_value
            )
        else:
            request_has_values = request['sourceServerSideEncryption']['customerKey'] == key_value
        assert request_has_values, 'The HTTP request must exercise the sensitive request fields'

        messages = [record.getMessage() for record in handler.buffer]
        has_private_values = any(
            value in message
            for value in (signing_value, header_value, key_value, response_value, message_value)
            for message in messages
        )
        assert not has_private_values, 'Private values must not appear in SDK tracing'
        assert any(
            'calling interpret_b2_error(' in message
            and f'status={status}' in message
            and f'code={code!r}' in message
            for message in messages
        )
