######################################################################
#
# File: test/unit/b2http/test_non_idempotent_create_retries.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import json

import pytest
import requests
from apiver_deps import B2RawHTTPApi
from apiver_deps_exception import B2ConnectionError, B2RequestTimeout, ServiceError, UnknownError

from b2sdk._internal.api_config import B2HttpApiConfig
from b2sdk._internal.b2http import B2Http, HttpCallback

API_URL = 'https://api.example.invalid'
AUTH_TOKEN = 'synthetic-account-token'

CREATE_CALLS = [
    pytest.param(
        'create_bucket',
        ('account-id', 'retry-test-bucket', 'allPrivate'),
        id='create-bucket',
    ),
    pytest.param(
        'create_key',
        ('account-id', ['readFiles'], 'retry-test-key', None, None, None),
        id='create-key',
    ),
    pytest.param(
        'start_large_file',
        ('bucket-id', 'large.bin', 'application/octet-stream', {}),
        id='start-large-file',
    ),
    pytest.param('copy_file', ('source-file-id', 'copy.bin'), id='copy-file'),
    pytest.param('hide_file', ('bucket-id', 'hidden.bin'), id='hide-file'),
    pytest.param('finish_large_file', ('large-file-id', ['part-sha1']), id='finish-large-file'),
]


class CommittedCreateTransport:
    """Model applied creates and failures before or after their server effect."""

    def __init__(self, failure):
        self.failures = [failure] if isinstance(failure, str) else failure
        self.requests = []
        self.created = []

    @staticmethod
    def response(url, status, payload):
        response = requests.Response()
        response.url = url
        response.status_code = status
        response._content = json.dumps(payload).encode()
        return response

    def request(self, method, url, *, data, **kwargs):
        assert method == 'POST'
        self.requests.append(json.loads(data))
        index = len(self.requests) - 1
        failure = self.failures[index] if index < len(self.failures) else None
        if failure == '429':
            response = self.response(
                url, 429, {'status': 429, 'code': 'too_many_requests', 'message': 'rate limited'}
            )
            response.headers['Retry-After'] = '2'
            return response
        if failure == 'connect-timeout':
            raise requests.ConnectTimeout('connection not established')
        if failure == 'new-connection':
            urllib3_errors = requests.packages.urllib3.exceptions
            reason = urllib3_errors.NewConnectionError(None, 'connection refused')
            raise requests.ConnectionError(urllib3_errors.MaxRetryError(None, url, reason))

        endpoint = url.rsplit('/', 1)[-1]
        if self.created and endpoint in ('b2_create_bucket', 'b2_finish_large_file'):
            # A replay cannot recover the successful first result for these calls.
            return self.response(
                url,
                400,
                {'status': 400, 'code': 'bad_request', 'message': 'resource already exists'},
            )
        self.created.append({'id': len(self.created) + 1})
        if failure == 'timeout':
            raise requests.ReadTimeout('response unavailable after commit')
        if failure == 'connection-lost':
            raise requests.ConnectionError('connection lost after commit')
        if failure == 'wrapped-connection-lost':
            urllib3_errors = requests.packages.urllib3.exceptions
            reason = urllib3_errors.ProtocolError(
                'connection lost after send', ConnectionResetError()
            )
            raise requests.ConnectionError(urllib3_errors.MaxRetryError(None, url, reason))
        if failure == '503':
            return self.response(
                url,
                503,
                {'status': 503, 'code': 'service_unavailable', 'message': 'response unavailable'},
            )
        return self.response(url, 200, self.created[-1])


@pytest.mark.parametrize('method, args', CREATE_CALLS)
@pytest.mark.parametrize(
    'failure', ['503', 'timeout', 'connection-lost', 'wrapped-connection-lost']
)
def test_committed_create_is_not_replayed_after_ambiguous_failure(method, args, failure, mocker):
    transport = CommittedCreateTransport(failure)
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    mocker.patch.object(http.session, 'request', side_effect=transport.request)
    # Backoff is not under test; the HTTP retry and translation code remain real.
    sleep = mocker.patch('b2sdk._internal.b2http.time.sleep')
    raw_api = B2RawHTTPApi(http)
    expected_error = {
        'timeout': B2RequestTimeout,
        '503': ServiceError,
        'connection-lost': B2ConnectionError,
        'wrapped-connection-lost': B2ConnectionError,
    }[failure]

    try:
        with pytest.raises(expected_error):
            getattr(raw_api, method)(API_URL, AUTH_TOKEN, *args)
    finally:
        http.session.close()

    assert len(transport.requests) == 1
    assert transport.created == [{'id': 1}]
    sleep.assert_not_called()


@pytest.mark.parametrize('failure', ['503', 'timeout'])
@pytest.mark.parametrize('operation', ['list-buckets', 'copy-part', 'http-explicit'])
def test_replayable_posts_still_retry(operation, failure, mocker):
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    raw_api = B2RawHTTPApi(http)
    requests_sent = []
    parts = {}
    if operation == 'copy-part':
        expected_result = {'fileId': 'large-file-id', 'partNumber': 1, 'contentSha1': 'part-sha1'}
    else:
        expected_result = {'buckets': []}

    def request(method, url, *, data, **kwargs):
        assert method == 'POST'
        payload = json.loads(data)
        requests_sent.append(payload)
        if operation == 'copy-part':
            parts[(payload['largeFileId'], payload['partNumber'])] = payload['sourceFileId']
        if len(requests_sent) == 1:
            if failure == 'timeout':
                raise requests.ReadTimeout('response unavailable')
            return CommittedCreateTransport.response(
                url,
                503,
                {'status': 503, 'code': 'service_unavailable', 'message': 'response unavailable'},
            )
        return CommittedCreateTransport.response(url, 200, expected_result)

    mocker.patch.object(http.session, 'request', side_effect=request)
    mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        if operation == 'copy-part':
            result = raw_api.copy_part(API_URL, AUTH_TOKEN, 'source-file-id', 'large-file-id', 1)
        elif operation == 'http-explicit':
            result = http.post_json_return_json(
                API_URL + '/b2api/v4/b2_list_buckets', {}, {'accountId': 'account-id'}, try_count=5
            )
        else:
            result = raw_api.list_buckets(API_URL, AUTH_TOKEN, 'account-id')
    finally:
        http.session.close()

    assert result == expected_result
    assert len(requests_sent) == 2
    assert requests_sent[0] == requests_sent[1]
    if operation == 'copy-part':
        assert parts == {('large-file-id', 1): 'source-file-id'}


def test_read_only_post_retains_five_attempt_limit(mocker):
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    raw_api = B2RawHTTPApi(http)
    request = mocker.patch.object(
        http.session,
        'request',
        side_effect=lambda method, url, **kwargs: CommittedCreateTransport.response(
            url, 503, {'status': 503, 'code': 'service_unavailable', 'message': 'unavailable'}
        ),
    )
    mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        with pytest.raises(ServiceError):
            raw_api.list_buckets(API_URL, AUTH_TOKEN, 'account-id')
    finally:
        http.session.close()

    assert request.call_count == 5


@pytest.mark.parametrize('failure', ['503', 'timeout', 'connection-lost'])
@pytest.mark.parametrize('entry_point', ['http', 'unclassified-raw-api'])
def test_json_post_without_retry_classification_is_not_replayed(entry_point, failure, mocker):
    transport = CommittedCreateTransport(failure)
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    mocker.patch.object(http.session, 'request', side_effect=transport.request)
    sleep = mocker.patch('b2sdk._internal.b2http.time.sleep')
    expected_error = {
        'timeout': B2RequestTimeout,
        '503': ServiceError,
        'connection-lost': B2ConnectionError,
    }[failure]
    try:
        with pytest.raises(expected_error):
            if entry_point == 'http':
                http.post_json_return_json(API_URL + '/new-operation', {}, {'name': 'candidate'})
            else:
                B2RawHTTPApi(http)._post_json(
                    API_URL, 'new-operation', AUTH_TOKEN, name='candidate'
                )
    finally:
        http.session.close()

    assert len(transport.requests) == 1
    assert transport.created == [{'id': 1}]
    sleep.assert_not_called()


@pytest.mark.parametrize('method, args', CREATE_CALLS)
@pytest.mark.parametrize('failure', ['429', 'connect-timeout', 'new-connection'])
def test_creates_retry_only_failures_before_the_server_effect(method, args, failure, mocker):
    transport = CommittedCreateTransport(failure)
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    mocker.patch.object(http.session, 'request', side_effect=transport.request)
    sleep = mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        result = getattr(B2RawHTTPApi(http), method)(API_URL, AUTH_TOKEN, *args)
    finally:
        http.session.close()

    assert result == {'id': 1}
    assert transport.created == [{'id': 1}]
    assert len(transport.requests) == 2
    sleep.assert_called_once_with(2 if failure == '429' else 1)


@pytest.mark.parametrize('method, args', CREATE_CALLS)
def test_create_retries_new_connection_without_shared_timeout_parent(method, args, mocker):
    # Simulate a future hierarchy while retaining real NewConnectionError instances.
    urllib3_errors = requests.packages.urllib3.exceptions

    class IndependentConnectTimeoutError(urllib3_errors.HTTPError):
        pass

    mocker.patch.object(urllib3_errors, 'ConnectTimeoutError', IndependentConnectTimeoutError)
    assert not issubclass(urllib3_errors.NewConnectionError, urllib3_errors.ConnectTimeoutError)
    transport = CommittedCreateTransport('new-connection')
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    mocker.patch.object(http.session, 'request', side_effect=transport.request)
    sleep = mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        result = getattr(B2RawHTTPApi(http), method)(API_URL, AUTH_TOKEN, *args)
    finally:
        http.session.close()

    assert len(transport.requests) == 2
    assert transport.requests[0] == transport.requests[1]
    assert transport.created == [{'id': 1}]
    assert result == {'id': 1}
    sleep.assert_called_once_with(1)


@pytest.mark.parametrize('method, args', CREATE_CALLS)
def test_create_retries_wrapped_connect_timeout(method, args, mocker):
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    requests_sent = []
    created = []

    def request(http_method, url, *, data, **kwargs):
        assert http_method == 'POST'
        requests_sent.append(json.loads(data))
        if len(requests_sent) == 1:
            urllib3_errors = requests.packages.urllib3.exceptions
            reason = urllib3_errors.ConnectTimeoutError(None, 'connection timed out')
            raise requests.ConnectionError(urllib3_errors.MaxRetryError(None, url, reason=reason))
        created.append({'id': len(created) + 1})
        return CommittedCreateTransport.response(url, 200, created[-1])

    mocker.patch.object(http.session, 'request', side_effect=request)
    sleep = mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        result = getattr(B2RawHTTPApi(http), method)(API_URL, AUTH_TOKEN, *args)
    finally:
        http.session.close()

    assert len(requests_sent) == 2
    assert requests_sent[0] == requests_sent[1]
    assert created == [{'id': 1}]
    assert result == {'id': 1}
    sleep.assert_called_once_with(1)


@pytest.mark.parametrize('failure', ['503', 'timeout', 'connection-lost'])
def test_ambiguous_create_is_not_replayed_after_a_safe_retry(failure, mocker):
    transport = CommittedCreateTransport(['429', failure])
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    mocker.patch.object(http.session, 'request', side_effect=transport.request)
    sleep = mocker.patch('b2sdk._internal.b2http.time.sleep')
    expected_error = {
        'timeout': B2RequestTimeout,
        '503': ServiceError,
        'connection-lost': B2ConnectionError,
    }[failure]
    try:
        with pytest.raises(expected_error):
            B2RawHTTPApi(http).create_key(
                API_URL, AUTH_TOKEN, 'account-id', ['readFiles'], 'retry-test-key', None, None, None
            )
    finally:
        http.session.close()

    assert len(transport.requests) == 2
    assert transport.created == [{'id': 1}]
    sleep.assert_called_once_with(2)


def test_binary_part_upload_retains_its_explicit_retry_policy(mocker):
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    bodies = []

    def request(method, url, *, data, **kwargs):
        assert method == 'POST'
        bodies.append(data)
        if len(bodies) == 1:
            return CommittedCreateTransport.response(
                url, 503, {'status': 503, 'code': 'service_unavailable', 'message': 'unavailable'}
            )
        return CommittedCreateTransport.response(
            url, 200, {'fileId': 'large-file-id', 'partNumber': 1}
        )

    mocker.patch.object(http.session, 'request', side_effect=request)
    mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        result = http.post_content_return_json(
            API_URL + '/b2api/v4/b2_upload_part', {}, b'part', try_count=2
        )
    finally:
        http.session.close()
    assert bodies == [b'part', b'part']
    assert result == {'fileId': 'large-file-id', 'partNumber': 1}


@pytest.mark.parametrize('after_json_failure', [False, True])
def test_content_metadata_does_not_enable_the_json_retry_policy(after_json_failure, mocker):
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    bodies = []

    def request(method, url, *, data, **kwargs):
        if url.endswith('b2_create_bucket'):
            return CommittedCreateTransport.response(
                url, 503, {'status': 503, 'code': 'service_unavailable', 'message': 'unavailable'}
            )
        bodies.append(data)
        if len(bodies) == 1:
            return CommittedCreateTransport.response(
                url, 503, {'status': 503, 'code': 'service_unavailable', 'message': 'unavailable'}
            )
        return CommittedCreateTransport.response(url, 200, {'ok': True})

    mocker.patch.object(http.session, 'request', side_effect=request)
    mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        if after_json_failure:
            with pytest.raises(ServiceError):
                http.post_json_return_json(
                    API_URL + '/b2api/v4/b2_create_bucket', {}, {'bucketName': 'candidate'}
                )
        result = http.post_content_return_json(
            API_URL + '/content', {}, b'part', try_count=2, post_params={'fileName': 'part.bin'}
        )
    finally:
        http.session.close()
    assert result == {'ok': True}
    assert bodies == [b'part', b'part']


def test_nested_content_request_keeps_its_own_retry_policy(mocker):
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    create_url = API_URL + '/b2api/v4/b2_create_bucket'
    attempts = {'create': 0, 'content': 0}
    nested_results = []

    def request(method, url, **kwargs):
        kind = 'create' if url == create_url else 'content'
        attempts[kind] += 1
        if kind == 'create' or attempts[kind] == 1:
            return CommittedCreateTransport.response(
                url, 503, {'status': 503, 'code': 'service_unavailable', 'message': 'unavailable'}
            )
        return CommittedCreateTransport.response(url, 200, {'ok': True})

    class NestedRequest(HttpCallback):
        def post_request(self, method, url, headers, response):
            if url == create_url:
                nested_results.append(
                    http.post_content_return_json(
                        API_URL + '/content',
                        {},
                        b'part',
                        try_count=2,
                        post_params={'fileName': 'part.bin'},
                    )
                )

    http.add_callback(NestedRequest())
    mocker.patch.object(http.session, 'request', side_effect=request)
    mocker.patch('b2sdk._internal.b2http.time.sleep')
    try:
        with pytest.raises(ServiceError):
            http.post_json_return_json(create_url, {}, {'bucketName': 'candidate'})
    finally:
        http.session.close()
    assert attempts == {'create': 1, 'content': 2}
    assert nested_results == [{'ok': True}]


def test_retry_classification_preserves_invalid_url_error_translation():
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    try:
        with pytest.raises(UnknownError):
            http.post_json_return_json('http://[invalid', {}, {'name': 'candidate'})
    finally:
        http.session.close()
