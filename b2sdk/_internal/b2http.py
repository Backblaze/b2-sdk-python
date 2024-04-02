######################################################################
#
# File: b2sdk/_internal/b2http.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime
import io
import json
import locale
import logging
import socket
import threading
import time
from contextlib import contextmanager
from random import random
from typing import Any, Callable

try:
    from typing_extensions import Literal
except ImportError:
    from typing import Literal

import requests
from requests.adapters import HTTPAdapter

from b2sdk.version import USER_AGENT

from .api_config import DEFAULT_HTTP_API_CONFIG, B2HttpApiConfig
from .exception import (
    B2ConnectionError,
    B2Error,
    B2RequestTimeout,
    B2RequestTimeoutDuringUpload,
    BadDateFormat,
    BrokenPipe,
    ClockSkew,
    ConnectionReset,
    PotentialS3EndpointPassedAsRealm,
    UnknownError,
    UnknownHost,
    interpret_b2_error,
)
from .requests import NotDecompressingResponse
from .utils.typing import JSON

LOCALE_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


def _print_exception(e, indent=''):
    """
    Used for debugging to print out nested exception structures.

    :param str indent: message prefix
    """
    print(indent + 'EXCEPTION', repr(e))
    print(indent + 'CLASS', type(e))
    for (i, a) in enumerate(e.args):
        print(indent + 'ARG %d: %s' % (i, repr(a)))
        if isinstance(a, Exception):
            _print_exception(a, indent + '        ')


@contextmanager
def setlocale(name):
    with LOCALE_LOCK:
        saved = locale.setlocale(locale.LC_ALL)
        try:
            yield locale.setlocale(locale.LC_ALL, name)
        finally:
            locale.setlocale(locale.LC_ALL, saved)


class ResponseContextManager:
    """
    A context manager that closes a requests.Response when done.
    """

    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self.response

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None


class HttpCallback:
    """
    A callback object that does nothing.  Overrides pre_request
    and/or post_request as desired.
    """

    def pre_request(self, method, url, headers):
        """
        Called before processing an HTTP request.

        Raises an exception if this request should not be processed.
        The exception raised must inherit from B2HttpCallbackPreRequestException.

        :param str method: str, one of: 'POST', 'GET', etc.
        :param str url: the URL that will be used
        :param dict headers: the header sent with the request

        """

    def post_request(self, method, url, headers, response):
        """
        Called after processing an HTTP request.
        Should not raise an exception.

        Raises an exception if this request should be treated as failing.
        The exception raised must inherit from B2HttpCallbackPostRequestException.

        :param str method: one of: 'POST', 'GET', etc.
        :param str url: the URL that will be used
        :param dict headers: the header sent with the request
        :param response: a response object from the requests library
        """


class ClockSkewHook(HttpCallback):
    def post_request(self, method, url, headers, response):
        """
        Raise an exception if the clock in the server is too different from the
        clock on the local host.

        The Date header contains a string that looks like: "Fri, 16 Dec 2016 20:52:30 GMT".

        :param str method: one of: 'POST', 'GET', etc.
        :param str url: the URL that will be used
        :param dict headers: the header sent with the request
        :param response: a response object from the requests library
        """
        # Make a string that uses month numbers instead of month names
        server_date_str = response.headers['Date']

        # Convert the server time to a datetime object
        try:
            with setlocale("C"):
                server_time = datetime.datetime.strptime(
                    server_date_str, '%a, %d %b %Y %H:%M:%S %Z'
                )
        except ValueError:
            logger.exception('server returned date in an inappropriate format')
            raise BadDateFormat(server_date_str)

        # Get the local time
        local_time = datetime.datetime.utcnow()

        # Check the difference.
        max_allowed = 10 * 60  # ten minutes, in seconds
        skew = local_time - server_time
        skew_seconds = skew.total_seconds()
        if max_allowed < abs(skew_seconds):
            raise ClockSkew(skew_seconds)


class B2Http:
    """
    A wrapper for the requests module.  Provides the operations
    needed to access B2, and handles retrying when the returned
    status is 503 Service Unavailable, 429 Too Many Requests, etc.

    The operations supported are:

    - post_json_return_json
    - post_content_return_json
    - get_content

    The methods that return JSON either return a Python dict or
    raise a subclass of B2Error.  They can be used like this:

    .. code-block:: python

       try:
           response_dict = b2_http.post_json_return_json(url, headers, params)
           ...
       except B2Error as e:
           ...

    Please note that the timeout/retry system, including class-level variables,
    is not a part of the interface and is subject to change.
    """

    CONNECTION_TIMEOUT = 3 + 6 + 12 + 24 + 1  # 4 standard tcp retransmissions + 1s latency
    TIMEOUT = 128
    TIMEOUT_FOR_COPY = 1200  # 20 minutes as server-side copy can take time
    TIMEOUT_FOR_UPLOAD = 128
    TRY_COUNT_DATA = 20
    TRY_COUNT_DOWNLOAD = 20
    TRY_COUNT_HEAD = 5
    TRY_COUNT_OTHER = 5

    def __init__(self, api_config: B2HttpApiConfig = DEFAULT_HTTP_API_CONFIG):
        """
        Initialize with a reference to the requests module, which makes
        it easy to mock for testing.
        """
        self.user_agent = self._get_user_agent(api_config.user_agent_append)
        self.session = api_config.http_session_factory()
        if not api_config.decode_content:
            self.session.adapters.clear()
            self.session.mount('', NotDecompressingHTTPAdapter())
        self.callbacks = []
        if api_config.install_clock_skew_hook:
            self.add_callback(ClockSkewHook())

    def add_callback(self, callback):
        """
        Add a callback that inherits from HttpCallback.

        :param callback: a callback to be added to a chain
        :type callback: callable
        """
        self.callbacks.append(callback)

    def request(
        self,
        method: Literal['POST', 'GET', 'HEAD'],
        url: str,
        headers: dict[str, str],
        data: io.BytesIO | bytes | None = None,
        try_count: int = TRY_COUNT_DATA,
        params: dict[str, str] | None = None,
        *,
        stream: bool = False,
        _timeout: int | None = None,
    ) -> requests.Response:
        """
        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.request('POST', url, headers, data)
               ...
           except B2Error as e:
               ...

        :param method: uppercase HTTP method name
        :param url: a URL to call
        :param headers: headers to send.
        :param data: raw bytes or a file-like object to send
        :param try_count: a number of retries
        :param params: a dict that will be converted to query string for GET requests or additional metadata for POST requests
        :param stream: if True, the response will be streamed
        :param _timeout: a timeout for the request in seconds if not default
        :return: final response
        :raises: B2Error if the request fails
        """
        method = method.upper()
        request_headers = {**headers, 'User-Agent': self.user_agent}

        def do_request():
            # This may retry, so each time we need to rewind the data back to the beginning.
            if data is not None and not isinstance(data, bytes):
                data.seek(0)
            self._run_pre_request_hooks(method, url, request_headers)
            response = self.session.request(
                method,
                url,
                headers=request_headers,
                data=data,
                params=params if method == 'GET' else None,
                timeout=(self.CONNECTION_TIMEOUT, _timeout or self.TIMEOUT_FOR_UPLOAD),
                stream=stream,
            )
            self._run_post_request_hooks(method, url, request_headers, response)
            return response

        return self._translate_and_retry(do_request, try_count, params)

    def request_content_return_json(
        self,
        method: Literal['POST', 'GET', 'HEAD'],
        url: str,
        headers: dict[str, str],
        data: io.BytesIO | bytes | None = None,
        try_count: int = TRY_COUNT_DATA,
        params: dict[str, str] | None = None,
        *,
        _timeout: int | None = None,
    ) -> JSON:
        """
        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.request_content_return_json('POST', url, headers, data)
               ...
           except B2Error as e:
               ...

        :param method: uppercase HTTP method name
        :param url: a URL to call
        :param headers: headers to send.
        :param data: raw bytes or a file-like object to send
        :return: decoded JSON
        """
        response = self.request(
            method,
            url,
            headers={
                **headers, 'Accept': 'application/json'
            },
            data=data,
            try_count=try_count,
            params=params,
            _timeout=_timeout
        )

        # Decode the JSON that came back.  If we've gotten this far,
        # we know we have a status of 200 OK.  In this case, the body
        # of the response is always JSON, so we don't need to handle
        # it being something else.
        try:
            return json.loads(response.content.decode('utf-8'))
        finally:
            response.close()

    def post_content_return_json(
        self,
        url: str,
        headers: dict[str, str],
        data: bytes | io.IOBase,
        try_count: int = TRY_COUNT_DATA,
        post_params: dict[str, str] | None = None,
        _timeout: int | None = None,
    ) -> JSON:
        """
        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.post_content_return_json(url, headers, data)
               ...
           except B2Error as e:
               ...

        :param str url: a URL to call
        :param dict headers: headers to send.
        :param data: a file-like object to send
        :return: a dict that is the decoded JSON
        """
        try:
            return self.request_content_return_json(
                'POST', url, headers, data, try_count, post_params, _timeout=_timeout
            )
        except B2RequestTimeout:
            # this forces a token refresh, which is necessary if request is still alive
            # on the server but has terminated for some reason on the client. See #79
            raise B2RequestTimeoutDuringUpload()

    def post_json_return_json(self, url, headers, params, try_count: int = TRY_COUNT_OTHER):
        """
        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.post_json_return_json(url, headers, params)
               ...
           except B2Error as e:
               ...

        :param str url: a URL to call
        :param dict headers: headers to send.
        :param dict params: a dict that will be converted to JSON
        :return: the decoded JSON document
        :rtype: dict
        """

        # This is not just b2_copy_file or b2_copy_part, but it would not
        # be good to find out by analyzing the url.
        # In the future a more generic system between raw_api and b2http
        # to indicate the timeouts should be designed.
        timeout = self.TIMEOUT_FOR_COPY

        data = json.dumps(params).encode()
        return self.post_content_return_json(
            url,
            {
                **headers,
                'Content-Type': 'application/json',
            },
            data,
            try_count,
            params,
            _timeout=timeout,
        )

    def get_content(self, url, headers, try_count: int = TRY_COUNT_DOWNLOAD):
        """
        Fetches content from a URL.

        Use like this:

        .. code-block:: python

           try:
               with b2_http.get_content(url, headers) as response:
                   for byte_data in response.iter_content(chunk_size=1024):
                       ...
           except B2Error as e:
               ...

        The response object is only guarantee to have:
            - headers
            - iter_content()

        :param str url: a URL to call
        :param dict headers: headers to send
        :param int try_count: a number or retries
        :return: Context manager that returns an object that supports iter_content()
        """
        response = self.request(
            'GET', url, headers=headers, try_count=try_count, stream=True, _timeout=self.TIMEOUT
        )
        return ResponseContextManager(response)

    def head_content(
        self,
        url: str,
        headers: dict[str, Any],
        try_count: int = TRY_COUNT_HEAD,
    ) -> requests.Response:
        """
        Does a HEAD instead of a GET for the URL.
        The response's content is limited to the headers.

        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.head_content(url, headers)
               ...
           except B2Error as e:
               ...

        The response object is only guaranteed to have:
            - headers

        :param str url: a URL to call
        :param dict headers: headers to send
        :param int try_count: a number or retries
        :return: HTTP response
        """
        return self.request('HEAD', url, headers=headers, try_count=try_count)

    @classmethod
    def _get_user_agent(cls, user_agent_append):
        if user_agent_append:
            return f'{USER_AGENT} {user_agent_append}'
        return USER_AGENT

    def _run_pre_request_hooks(self, method, url, headers):
        for callback in self.callbacks:
            callback.pre_request(method, url, headers)

    def _run_post_request_hooks(self, method, url, headers, response):
        for callback in self.callbacks:
            callback.post_request(method, url, headers, response)

    @classmethod
    def _translate_errors(cls, fcn, post_params=None):
        """
        Call the given function, turning any exception raised into the right
        kind of B2Error.

        :param dict post_params: request parameters
        """
        response = None
        try:
            response = fcn()
            if response.status_code not in (200, 206):
                # Decode the error object returned by the service
                try:
                    error = json.loads(response.content.decode('utf-8')) if response.content else {}
                    if not isinstance(error, dict):
                        raise ValueError('json error value is not a dict')
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                    logger.error('failed to decode error response: %r', response.content)
                    # When the user points to an S3 endpoint, he won't receive the JSON error
                    # he expects. In that case, we can provide at least a hint of "what happened".
                    # s3 url has the form of e.g. https://s3.us-west-000.backblazeb2.com
                    if '://s3.' in response.url:
                        raise PotentialS3EndpointPassedAsRealm(response.content)
                    error = {
                        'message': response.content.decode('utf-8', errors='replace'),
                        'code': 'non_json_response',
                    }
                extra_error_keys = error.keys() - ('code', 'status', 'message')
                if extra_error_keys:
                    logger.debug(
                        'received error has extra (unsupported) keys: %s', extra_error_keys
                    )

                try:
                    status = int(error.get('status', response.status_code))
                    if status != response.status_code:
                        raise ValueError('status code is not equal to the one in the response')
                except (TypeError, ValueError) as exc:
                    logger.warning(
                        'Inconsistent status codes returned by the server %r != %r; parsing exception: %r',
                        error.get('status'), response.status_code, exc
                    )
                    status = response.status_code

                raise interpret_b2_error(
                    status,
                    str(error['code']) if 'code' in error else None,
                    str(error['message']) if 'message' in error else None,
                    response.headers,
                    post_params,
                )
            return response

        except B2Error:
            raise  # pass through exceptions from just above

        except requests.ConnectionError as e0:
            e1 = e0.args[0]
            if isinstance(e1, requests.packages.urllib3.exceptions.MaxRetryError):
                msg = e1.args[0]
                if 'nodename nor servname provided, or not known' in msg:
                    # Unknown host, or DNS failing.  In the context of calling
                    # B2, this means that something is down between here and
                    # Backblaze, so we treat it like 503 Service Unavailable.
                    raise UnknownHost()
            elif isinstance(e1, requests.packages.urllib3.exceptions.ProtocolError):
                e2 = e1.args[1]
                if isinstance(e2, socket.error):
                    if len(e2.args) >= 2 and e2.args[1] == 'Broken pipe':
                        # Broken pipes are usually caused by the service rejecting
                        # an upload request for cause, so we use a 400 Bad Request
                        # code.
                        raise BrokenPipe()
            raise B2ConnectionError(str(e0))

        except requests.Timeout as e:
            raise B2RequestTimeout(str(e))

        except Exception as e:
            text = repr(e)

            # This is a special case to handle when urllib3 doesn't translate
            # ECONNRESET into something that requests can turn into something
            # we understand.  The SysCallError is from the optional library
            # pyOpenSsl, which we don't require, so we can't import it and
            # catch it explicitly.
            #
            # The text from one such error looks like this: SysCallError(104, 'ECONNRESET')
            if text.startswith('SysCallError'):
                if 'ECONNRESET' in text:
                    raise ConnectionReset()

            logger.exception('_translate_errors has intercepted an unexpected exception')
            raise UnknownError(text)

    @classmethod
    def _translate_and_retry(
        cls, fcn: Callable, try_count: int, post_params: dict[str, Any] | None = None
    ):
        """
        Try calling fcn try_count times, retrying only if
        the exception is a retryable B2Error.

        :param fcn: request function to call
        :param try_count: a number of retries
        :param post_params: request parameters
        """
        # For all but the last try, catch the exception.
        wait_time = 1.0
        max_wait_time = 64
        for _ in range(try_count - 1):
            try:
                return cls._translate_errors(fcn, post_params)
            except B2Error as e:
                if not e.should_retry_http():
                    raise
                logger.debug(str(e), exc_info=True)
                if e.retry_after_seconds is not None:
                    sleep_duration = e.retry_after_seconds
                    sleep_reason = 'server asked us to'
                else:
                    sleep_duration = wait_time
                    sleep_reason = 'that is what the default exponential backoff is'

                logger.info(
                    'Pausing thread for %i seconds because %s',
                    sleep_duration,
                    sleep_reason,
                )
                time.sleep(sleep_duration)

                # Set up wait time for the next iteration
                wait_time *= 1.5
                if wait_time > max_wait_time:
                    # avoid clients synchronizing and causing a wave
                    # of requests when connectivity is restored
                    wait_time = max_wait_time + random()

        # If the last try gets an exception, it will be raised.
        return cls._translate_errors(fcn, post_params)


class NotDecompressingHTTPAdapter(HTTPAdapter):
    """
    HTTP adapter that uses :class:`b2sdk._internal.requests.NotDecompressingResponse` instead of the default
    :code:`requests.Response` class.
    """

    def build_response(self, req, resp):
        return NotDecompressingResponse.from_builtin_response(super().build_response(req, resp))
