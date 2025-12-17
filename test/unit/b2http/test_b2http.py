######################################################################
# File: test/unit/b2http/test_b2http.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime
import locale
import sys
from unittest.mock import MagicMock, call

import apiver_deps
import pytest
import requests
import responses
from apiver_deps import USER_AGENT, B2Http, B2HttpApiConfig, ClockSkewHook
from apiver_deps_exception import (
    B2ConnectionError,
    B2RequestTimeout,
    BadDateFormat,
    BadJson,
    BadRequest,
    BrokenPipe,
    ClockSkew,
    ConnectionReset,
    PotentialS3EndpointPassedAsRealm,
    ServiceError,
    TooManyRequests,
    UnknownError,
    UnknownHost,
)
from pytest_mock import MockerFixture
from responses import matchers

from b2sdk._internal.b2http import setlocale

from ..test_base import TestBase


@pytest.fixture
def user_agent_append():
    return None


@pytest.fixture
def b2_http(user_agent_append: str | None):
    if apiver_deps.V <= 1:
        return B2Http(
            requests,
            install_clock_skew_hook=False,
            user_agent_append=user_agent_append,
        )
    else:
        return B2Http(
            B2HttpApiConfig(
                requests.Session,
                install_clock_skew_hook=False,
                user_agent_append=user_agent_append,
            )
        )


def _mock_error_response(
    url: str,
    *,
    method: str = 'GET',
    status: int = 400,
    code: str = 'server_busy',
    message: str = 'dummy',
    headers: dict | None = None,
):
    responses.add(
        method,
        url,
        status=status,
        json={'status': status, 'code': code, 'message': message},
        adding_headers=headers,
    )


class TestTranslateErrors:
    URL = 'http://example.com'

    @responses.activate
    def test_ok(self, b2_http: B2Http):
        data = {'foo': 'bar'}

        responses.get(self.URL, json=data)

        response = b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert response.json() == data

    @responses.activate
    def test_partial_content(self, b2_http: B2Http):
        data = {'foo': 'bar'}

        responses.get(self.URL, status=206, json=data)

        response = b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert response.json() == data

    @responses.activate
    def test_b2_error(self, b2_http: B2Http):
        code = 'server_busy'
        message = 'busy'

        _mock_error_response(self.URL, status=503, code=code, message=message)

        with pytest.raises(ServiceError) as exc:
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert '503' in str(exc.value)
        assert code in str(exc.value)
        assert message in str(exc.value)

    @responses.activate
    def test_broken_pipe(self, b2_http: B2Http):
        responses.get(
            self.URL,
            body=requests.ConnectionError(
                requests.packages.urllib3.exceptions.ProtocolError(
                    'dummy', OSError(20, 'Broken pipe')
                )
            ),
        )

        with pytest.raises(BrokenPipe):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_unknown_host(self, b2_http: B2Http):
        responses.get(
            self.URL,
            body=requests.ConnectionError(
                requests.packages.urllib3.exceptions.MaxRetryError(
                    'AAA nodename nor servname provided, or not known AAA', 'http://example.com'
                )
            ),
        )

        with pytest.raises(UnknownHost):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_request_timeout(self, b2_http: B2Http):
        responses.get(
            self.URL,
            body=requests.ConnectionError(
                requests.packages.urllib3.exceptions.ProtocolError(
                    'dummy', TimeoutError('The write operation timed out')
                )
            ),
        )

        with pytest.raises(B2RequestTimeout):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_connection_error(self, b2_http: B2Http):
        responses.get(
            self.URL,
            body=requests.ConnectionError('a message'),
        )

        with pytest.raises(B2ConnectionError):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_connection_reset(self, b2_http: B2Http):
        class SysCallError(Exception):
            pass

        responses.get(
            self.URL,
            body=SysCallError('(104, ECONNRESET)'),
        )

        with pytest.raises(ConnectionReset):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_unknown_error(self, b2_http: B2Http):
        responses.get(self.URL, body=Exception('a message'))

        with pytest.raises(UnknownError):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_too_many_requests(self, b2_http: B2Http):
        _mock_error_response(
            self.URL,
            status=429,
            code='Too Many Requests',
            message='retry after some time',
            headers={'Retry-After': '1'},
        )

        with pytest.raises(TooManyRequests):
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

    @responses.activate
    def test_invalid_json(self, b2_http: B2Http):
        content = b'{' * 500

        responses.get(self.URL, status=400, body=content)

        with pytest.raises(BadRequest) as exc_info:
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert str(exc_info.value) == f'{content.decode()} (non_json_response)'

    @responses.activate
    def test_potential_s3_endpoint_passed_as_realm(self, b2_http: B2Http):
        url = 'https://s3.us-west-000.backblazeb2.com'

        responses.get(
            url, status=400, body=b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        )

        with pytest.raises(PotentialS3EndpointPassedAsRealm):
            b2_http.request(responses.GET, url, {}, try_count=1)

    @pytest.mark.apiver(to_ver=2)
    @responses.activate
    def test_bucket_id_not_found(self, b2_http: B2Http):
        from b2sdk.v2.exception import BucketIdNotFound, v3BucketIdNotFound

        responses.get(self.URL, body=v3BucketIdNotFound('bucket_id'))

        with pytest.raises(BucketIdNotFound) as exc_info:
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert str(exc_info.value) == 'Bucket with id=bucket_id not found (bad_bucket_id)'

    @responses.activate
    def test_b2_error__nginx_html(self, b2_http: B2Http):
        """
        While errors with HTML description should not happen, we should not crash on them.
        """
        content = b'<html><body><h1>502 Bad Gateway</h1></body></html>'

        responses.get(self.URL, status=502, body=content)

        with pytest.raises(ServiceError) as exc_info:
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert content.decode('utf-8') in str(exc_info.value)

    @responses.activate
    def test_b2_error__invalid_error_format(self, b2_http: B2Http):
        """
        Handling of invalid error format.

        If server returns valid JSON, but not matching B2 error schema, we should still raise ServiceError.
        """

        # valid JSON, but not a valid B2 error (it should be a dict, not a list)
        responses.get(self.URL, status=503, body=b'[]')

        with pytest.raises(ServiceError) as exc_info:
            b2_http.request(responses.GET, self.URL, {}, try_count=1)
        assert '503' in str(exc_info.value)

    @responses.activate
    def test_b2_error__invalid_error_values(self, b2_http: B2Http):
        """
        Handling of invalid error values.

        If server returns valid JSON, but not matching B2 error schema, we should still raise ServiceError.
        """

        # valid JSON, but not a valid B2 error (code and status values (and therefore types!) are swapped)
        responses.get(
            self.URL,
            status=503,
            body=b'{"code": 503, "message": "Service temporarily unavailable", "status": "service_unavailable"}',
        )

        with pytest.raises(ServiceError) as exc_info:
            b2_http.request(responses.GET, self.URL, {}, try_count=1)

        assert '503 Service temporarily unavailable' in str(exc_info.value)


class TestTranslateAndRetry:
    URL = 'http://example.com'

    @pytest.fixture
    def mock_time(self, mocker: MockerFixture):
        return mocker.patch('time.sleep')

    @responses.activate
    def test_works_first_try(self, b2_http: B2Http, mock_time: MagicMock):
        data = {'foo': 'bar'}

        responses.get(self.URL, json=data)

        response = b2_http.request(responses.GET, self.URL, {})

        assert response.json() == data

        mock_time.assert_not_called()

    @responses.activate
    def test_non_retryable(self, b2_http: B2Http, mock_time: MagicMock):
        _mock_error_response(self.URL, status=400, code='bad_json')

        with pytest.raises(BadJson):
            b2_http.request(responses.GET, self.URL, {})

        mock_time.assert_not_called()

    @responses.activate
    def test_works_second_try_service_error(self, b2_http: B2Http, mock_time: MagicMock):
        responses.get(self.URL, body=requests.ConnectionError('oops'))
        responses.get(self.URL)

        b2_http.request(responses.GET, self.URL, {})
        mock_time.assert_called_once_with(1.0)

    @responses.activate
    def test_works_second_try_status(self, b2_http: B2Http, mock_time: MagicMock):
        _mock_error_response(self.URL, status=503)
        responses.get(self.URL)

        b2_http.request(responses.GET, self.URL, {})
        mock_time.assert_called_once_with(1.0)

    @responses.activate
    def test_never_works(self, b2_http: B2Http, mock_time: MagicMock):
        _mock_error_response(self.URL, status=503)
        _mock_error_response(self.URL, status=503)
        _mock_error_response(self.URL, status=503)
        responses.get(self.URL)

        with pytest.raises(ServiceError):
            b2_http.request(responses.GET, self.URL, {}, try_count=3)

        assert mock_time.mock_calls == [call(1.0), call(1.5)]

    @responses.activate
    def test_too_many_requests_works_after_sleep(self, b2_http: B2Http, mock_time: MagicMock):
        _mock_error_response(self.URL, status=429, headers={'Retry-After': '2'})
        responses.get(self.URL)

        b2_http.request(responses.GET, self.URL, {})
        mock_time.assert_called_once_with(2)

    @responses.activate
    def test_too_many_requests_failed_after_sleep(self, b2_http: B2Http, mock_time: MagicMock):
        _mock_error_response(self.URL, status=429, headers={'Retry-After': '2'})
        _mock_error_response(self.URL, status=429, headers={'Retry-After': '5'})

        with pytest.raises(TooManyRequests):
            b2_http.request(responses.GET, self.URL, {}, try_count=2)
        mock_time.assert_called_once_with(2)

    @responses.activate
    def test_too_many_requests_retry_header_combination_one(
        self, b2_http: B2Http, mock_time: MagicMock
    ):
        # If the first response had header, and the second did not, but the third has header again, what should happen?
        _mock_error_response(self.URL, status=429, headers={'Retry-After': '2'})
        _mock_error_response(self.URL, status=429)
        _mock_error_response(self.URL, status=429, headers={'Retry-After': '2'})
        responses.get(self.URL)

        b2_http.request(responses.GET, self.URL, {}, try_count=4)
        assert mock_time.mock_calls == [call(2), call(1.5), call(2)]

    @responses.activate
    def test_too_many_requests_retry_header_combination_two(
        self, b2_http: B2Http, mock_time: MagicMock
    ):
        # If the first response didn't have a header, second one has, and third one doesn't have, what should happen?
        _mock_error_response(self.URL, status=429)
        _mock_error_response(self.URL, status=429, headers={'Retry-After': '5'})
        _mock_error_response(self.URL, status=429)
        responses.get(self.URL)

        b2_http.request(responses.GET, self.URL, {}, try_count=4)
        assert mock_time.mock_calls == [call(1.0), call(5), call(2.25)]

    @responses.activate
    def test_service_error_during_upload_no_retries(self, b2_http: B2Http, mock_time: MagicMock):
        _mock_error_response(self.URL, method=responses.POST, status=503)
        responses.post(self.URL)

        headers = {'X-Bz-Content-Sha1': '1234'}

        with pytest.raises(ServiceError):
            b2_http.request(responses.POST, self.URL, headers)

        mock_time.assert_not_called()

    @responses.activate
    def test_request_timeout_during_upload_no_retries(self, b2_http: B2Http, mock_time: MagicMock):
        responses.post(
            self.URL,
            body=requests.ConnectionError(
                requests.packages.urllib3.exceptions.ProtocolError(
                    'dummy', TimeoutError('The write operation timed out')
                )
            ),
        )
        responses.post(self.URL)

        headers = {'X-Bz-Content-Sha1': '1234'}

        with pytest.raises(B2RequestTimeout):
            b2_http.request(responses.POST, self.URL, headers)

        mock_time.assert_not_called()


class TestB2Http:
    URL = 'http://example.com'
    HEADERS = dict(my_header='my_value')
    EXPECTED_HEADERS = {'my_header': 'my_value', 'User-Agent': USER_AGENT}
    EXPECTED_JSON_HEADERS = {
        **EXPECTED_HEADERS,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    PARAMS = dict(fileSize=100)

    @responses.activate
    def test_post_json_return_json(self, b2_http: B2Http):
        data = {'color': 'blue'}

        responses.post(
            self.URL,
            json=data,
            match=(
                matchers.header_matcher(
                    self.EXPECTED_JSON_HEADERS,
                ),
                matchers.json_params_matcher(self.PARAMS),
            ),
        )

        assert (
            b2_http.post_json_return_json(self.URL, self.HEADERS, self.PARAMS, try_count=1) == data
        )

    @responses.activate
    def test_callback(self, b2_http: B2Http):
        callback = MagicMock()
        callback.pre_request = MagicMock()
        callback.post_request = MagicMock()
        b2_http.add_callback(callback)

        responses.post(self.URL, json={'color': 'blue'})

        b2_http.post_json_return_json(self.URL, self.HEADERS, self.PARAMS, try_count=1)

        callback.pre_request.assert_called_with(
            'POST', 'http://example.com', self.EXPECTED_JSON_HEADERS
        )
        callback.post_request.assert_called_with(
            'POST', 'http://example.com', self.EXPECTED_JSON_HEADERS, responses.calls[0].response
        )

    def test_get_content(self, b2_http: B2Http):
        close_mock = MagicMock()

        def response_callback(resp):
            # mock the response close method so that we can can check if it has been called later
            resp.close = close_mock
            return resp

        with responses.RequestsMock(response_callback=response_callback) as m:
            m.get(
                self.URL,
                match=(
                    matchers.header_matcher(self.EXPECTED_HEADERS),
                    matchers.request_kwargs_matcher(
                        {
                            'stream': True,
                            'timeout': (B2Http.CONNECTION_TIMEOUT, B2Http.TIMEOUT),
                        }
                    ),
                ),
            )

            with b2_http.get_content(self.URL, self.HEADERS, try_count=1) as r:
                assert r is m.calls[0].response

            # prevent premature close() on requests.Response
            m.calls[0].response.close.assert_not_called()  # type: ignore

    @responses.activate
    def test_head_content(self, b2_http: B2Http):
        responses.head(
            self.URL,
            headers={'color': 'blue'},
            match=(matchers.header_matcher(self.EXPECTED_HEADERS),),
        )

        response = b2_http.head_content(
            self.URL,
            self.HEADERS,
        )

        assert response.headers['color'] == 'blue'


class TestB2HttpUserAgentAppend(TestB2Http):
    UA_APPEND = 'ua_extra_string'
    EXPECTED_HEADERS = {**TestB2Http.EXPECTED_HEADERS, 'User-Agent': f'{USER_AGENT} {UA_APPEND}'}
    EXPECTED_JSON_HEADERS = {
        **TestB2Http.EXPECTED_JSON_HEADERS,
        'User-Agent': EXPECTED_HEADERS['User-Agent'],
    }

    @pytest.fixture
    def user_agent_append(self):
        return self.UA_APPEND


class TestSetLocaleContextManager(TestBase):
    def test_set_locale_context_manager(self):
        # C.UTF-8 on Ubuntu 18.04 Bionic, C.utf8 on Ubuntu 22.04 Jammy
        # Neither macOS nor Windows have C.UTF-8 locale, and they use `en_US.UTF-8`.
        # Since Python 3.12, locale.normalize no longer falls back
        # to the `en_US` version, so we're providing it here manually.
        test_locale = locale.normalize('C.UTF-8' if sys.platform == 'linux' else 'en_US.UTF-8')
        other_locale = 'C'

        saved = locale.setlocale(locale.LC_ALL)
        if saved == test_locale:
            test_locale, other_locale = other_locale, test_locale

        locale.setlocale(locale.LC_ALL, other_locale)
        with setlocale(test_locale):
            assert locale.setlocale(category=locale.LC_ALL) == test_locale
        locale.setlocale(locale.LC_ALL, saved)


class TestClockSkewHook(TestBase):
    def test_bad_format(self):
        response = MagicMock()
        response.headers = {'Date': 'bad format'}
        with self.assertRaises(BadDateFormat):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_bad_month(self):
        response = MagicMock()
        response.headers = {'Date': 'Fri, 16 XXX 2016 20:52:30 GMT'}
        with self.assertRaises(BadDateFormat):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_no_skew(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        now_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        response = MagicMock()
        response.headers = {'Date': now_str}
        ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_positive_skew(self):
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=11)
        now_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        response = MagicMock()
        response.headers = {'Date': now_str}
        with self.assertRaises(ClockSkew):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_negative_skew(self):
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=-11)
        now_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        response = MagicMock()
        response.headers = {'Date': now_str}
        with self.assertRaises(ClockSkew):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)
