######################################################################
#
# File: test/unit/b2http/test_retry_after_bounds.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
import responses

from b2sdk._internal.api_config import B2HttpApiConfig
from b2sdk._internal.b2http import B2Http


@pytest.mark.parametrize('status', [429, 503])
@pytest.mark.parametrize(
    'retry_after, expected_delay',
    [
        pytest.param('-5', 0, id='negative'),
        pytest.param('-' + '9' * 5000, 0, id='very-long-negative'),
        pytest.param('0', 0, id='zero'),
        pytest.param('2', 2, id='ordinary'),
        pytest.param('200', 200, id='existing-200-second-delay'),
        pytest.param('300', 300, id='at-cap'),
        pytest.param('301', 300, id='above-cap'),
        pytest.param('999999999999999999999999', 300, id='huge'),
        pytest.param('9' * 5000, 300, id='very-long-positive'),
        pytest.param('0' * 5000 + '2', 2, id='very-long-zero-padding'),
        pytest.param('not-a-delay', 1, id='invalid-fallback'),
        pytest.param('Wed, 21 Oct 2015 07:28:00 GMT', 1, id='date-fallback'),
    ],
)
@responses.activate
def test_retry_after_delay_is_bounded(status, retry_after, expected_delay, mocker):
    url = 'https://api.example.invalid/resource'
    responses.get(
        url,
        status=status,
        json={'status': status, 'code': 'service_unavailable', 'message': 'retry later'},
        adding_headers={'Retry-After': retry_after},
    )
    responses.get(url, status=200, json={'ok': True})
    delays = []

    def record_sleep(delay):
        if delay < 0:
            raise ValueError('sleep length must be non-negative')
        delays.append(delay)

    mocker.patch('b2sdk._internal.b2http.time.sleep', side_effect=record_sleep)
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    try:
        response = http.request('GET', url, headers={}, try_count=2)
    finally:
        http.session.close()

    assert response.json() == {'ok': True}
    assert len(responses.calls) == 2
    if expected_delay == 0:
        assert delays in ([], [0])
    else:
        assert delays == [expected_delay]


@pytest.mark.parametrize(
    'retry_after, expected_delay', [('-5', 0), ('999999999999999', 300), ('200', 200)]
)
@responses.activate
def test_create_rate_limit_uses_bounded_retry_after(retry_after, expected_delay, mocker):
    import json

    from b2sdk._internal.raw_api import B2RawHTTPApi

    api_url = 'https://api.example.invalid'
    url = api_url + '/b2api/v4/b2_create_bucket'
    attempts = []
    created = []

    def handle_create(request):
        attempts.append(json.loads(request.body))
        if len(attempts) == 1:
            return (
                429,
                {'Content-Type': 'application/json', 'Retry-After': retry_after},
                json.dumps({'status': 429, 'code': 'too_many_requests', 'message': 'retry later'}),
            )
        created.append('bucket-id')
        return 200, {'Content-Type': 'application/json'}, json.dumps({'bucketId': created[-1]})

    responses.add_callback(responses.POST, url, callback=handle_create)
    delays = []
    mocker.patch('b2sdk._internal.b2http.time.sleep', side_effect=delays.append)
    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    try:
        result = B2RawHTTPApi(http).create_bucket(
            api_url, 'synthetic-token', 'account-id', 'retry-test-bucket', 'allPrivate'
        )
    finally:
        http.session.close()

    assert result == {'bucketId': 'bucket-id'}
    assert created == ['bucket-id']
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    if expected_delay == 0:
        assert delays in ([], [0])
    else:
        assert delays == [expected_delay]
