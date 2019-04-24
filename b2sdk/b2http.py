######################################################################
#
# File: b2sdk/b2http.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import arrow
import logging
import json
import socket

import requests
import six
import time

from .exception import (
    B2Error, BadDateFormat, BrokenPipe, B2ConnectionError, B2RequestTimeout, ClockSkew,
    ConnectionReset, interpret_b2_error, UnknownError, UnknownHost
)
from .version import USER_AGENT
from six.moves import range

logger = logging.getLogger(__name__)


def _print_exception(e, indent=''):
    """
    Used for debugging to print out nested exception structures.

    :param indent: message prefix
    :type indent: str
    """
    print(indent + 'EXCEPTION', repr(e))
    print(indent + 'CLASS', type(e))
    for (i, a) in enumerate(e.args):
        print(indent + 'ARG %d: %s' % (i, repr(a)))
        if isinstance(a, Exception):
            _print_exception(a, indent + '        ')


def _translate_errors(fcn, post_params=None):
    """
    Calls the given function, turning any exception raised into the right
    kind of B2Error.

    :param dict post_params: request parameters
    """
    try:
        response = fcn()
        if response.status_code not in [200, 206]:
            # Decode the error object returned by the service
            error = json.loads(response.content.decode('utf-8'))
            raise interpret_b2_error(
                int(error['status']), error['code'], error['message'], post_params
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


def _translate_and_retry(fcn, try_count, post_params=None):
    """
    Try calling fcn try_count times, retrying only if
    the exception is a retryable B2Error.

    :param int try_count: a number of retries
    :param dict post_params: request parameters
    """
    # For all but the last try, catch the exception.
    wait_time = 1.0
    for _ in range(try_count - 1):
        try:
            return _translate_errors(fcn, post_params)
        except B2Error as e:
            if not e.should_retry_http():
                raise
            time.sleep(wait_time)
            wait_time *= 1.5

    # If the last try gets an exception, it will be raised.
    return _translate_errors(fcn, post_params)


class ResponseContextManager(object):
    """
    Context manager that closes a requests.Response when done.
    """

    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self.response

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.response.close()


class HttpCallback(object):
    """
    A callback object that does nothing.  Override pre_request
    and/or post_request as desired.
    """

    def pre_request(self, method, url, headers):
        """
        Called before processing an HTTP request.

        Raises an exception if this request should not be processed.
        The exception raised must inherit from B2HttpCallbackPreRequestException.

        :param str method: str, One of: 'POST', 'GET', etc.
        :param str url: The URL that will be used.
        :param dict headers: The header sent with the request.

        """

    def post_request(self, method, url, headers, response):
        """
        Called after processing an HTTP request.
        Should not raise an exception.

        Raises an exception if this request should be treated as failing.
        The exception raised must inherit from B2HttpCallbackPostRequestException.

        :param str method: one of: 'POST', 'GET', etc.
        :param str url: the URL that will be used.
        :param dict headers: the header sent with the request.
        :param response: a response object from the requests library.
        """


class ClockSkewHook(HttpCallback):
    def post_request(self, method, url, headers, response):
        """
        Raises an exception if the clock in the server is too different from the
        clock on the local host.

        The Date header contains a string that looks like: "Fri, 16 Dec 2016 20:52:30 GMT".

        :param str method: one of: 'POST', 'GET', etc.
        :param str url: the URL that will be used.
        :param dict headers: the header sent with the request.
        :param response: a response object from the requests library.
        """
        # Make a string that uses month numbers instead of month names
        server_date_str = response.headers['Date']

        # Convert the server time to a datetime object
        try:
            server_time = arrow.get(
                server_date_str, 'ddd, DD MMM YYYY HH:mm:ss ZZZ'
            )  # this, unlike datetime.datetime.strptime, always uses English locale
        except arrow.parser.ParserError:
            logger.exception('server returned date in an inappropriate format')
            raise BadDateFormat(server_date_str)

        # Get the local time
        local_time = arrow.utcnow()

        # Check the difference.
        max_allowed = 10 * 60  # ten minutes, in seconds
        skew = local_time - server_time
        skew_seconds = skew.total_seconds()
        if max_allowed < abs(skew_seconds):
            raise ClockSkew(skew_seconds)


class B2Http(object):
    """
    A wrapper for the requests module.  Provides the operations
    needed to access B2, and handles retrying when the returned
    status is 503 Service Unavailable, 429 Too Many Requests etc

    The operations supported are:
       - post_json_return_json
       - post_content_return_json
       - get_content

    The methods that return JSON either return a Python dict  or
    raise a subclass of B2Error.  They can be used like this:

    .. code-block:: python

       try:
           response_dict = b2_http.post_json_return_json(url, headers, params)
           ...
       except B2Error as e:
           ...

    """

    # timeout for HTTP GET/POST requests
    TIMEOUT = 130

    def __init__(self, requests_module=None, install_clock_skew_hook=True):
        """
        Initialize with a reference to the requests module, which makes
        it easy to mock for testing.

        :param requests_module: a reference to requests module
        :param bool install_clock_skew_hook: if True, install a clock skew hook
        """
        requests_to_use = requests_module or requests
        self.session = requests_to_use.Session()
        self.callbacks = []
        if install_clock_skew_hook:
            self.add_callback(ClockSkewHook())

    def add_callback(self, callback):
        """
        Adds a callback that inherits from HttpCallback.

        :param callback: a callback to be added to a chain
        :type callback: callable
        """
        self.callbacks.append(callback)

    def post_content_return_json(self, url, headers, data, try_count=5, post_params=None):
        """
        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.post_content_return_json(url, headers, data)
               ...
           except B2Error as e:
               ...

        :param str url: URL to call
        :param dict headers: headers to send.
        :param data: bytes (Python 3) or str (Python 2), or a file-like object, to send
        :return: a dict that is the decoded JSON
        :rtype: dict
        """
        # Make the headers we'll send by adding User-Agent to what
        # the caller provided.  Make a copy before modifying.
        headers = dict(headers)  # make copy before modifying
        headers['User-Agent'] = USER_AGENT

        # Do the HTTP POST.  This may retry, so each post needs to
        # rewind the data back to the beginning.
        def do_post():
            data.seek(0)
            self._run_pre_request_hooks('POST', url, headers)
            response = self.session.post(url, headers=headers, data=data, timeout=self.TIMEOUT)
            self._run_post_request_hooks('POST', url, headers, response)
            return response

        response = _translate_and_retry(do_post, try_count, post_params)

        # Decode the JSON that came back.  If we've gotten this far,
        # we know we have a status of 200 OK.  In this case, the body
        # of the response is always JSON, so we don't need to handle
        # it being something else.
        try:
            return json.loads(response.content.decode('utf-8'))
        finally:
            response.close()

    def post_json_return_json(self, url, headers, params, try_count=5):
        """
        Use like this:

        .. code-block:: python

           try:
               response_dict = b2_http.post_json_return_json(url, headers, params)
               ...
           except B2Error as e:
               ...

        :param str url: URL to call
        :param dict headers: headers to send.
        :param dict params: a dict that will be converted to JSON
        :return: the decoded JSON document
        :rtype: dict
        """
        data = six.BytesIO(six.b(json.dumps(params)))
        return self.post_content_return_json(url, headers, data, try_count, params)

    def get_content(self, url, headers, try_count=5):
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

        :param str url: URL to call
        :param dict headers: headers to send
        :param int try_count: a number or retries
        :return: Context manager that returns an object that supports iter_content()
        """
        # Make the headers we'll send by adding User-Agent to what
        # the caller provided.  Make a copy before modifying.
        headers = dict(headers)  # make copy before modifying
        headers['User-Agent'] = USER_AGENT

        # Do the HTTP GET.
        def do_get():
            self._run_pre_request_hooks('GET', url, headers)
            response = self.session.get(url, headers=headers, stream=True, timeout=self.TIMEOUT)
            self._run_post_request_hooks('GET', url, headers, response)
            return response

        response = _translate_and_retry(do_get, try_count, None)
        return ResponseContextManager(response)

    def _run_pre_request_hooks(self, method, url, headers):
        for callback in self.callbacks:
            callback.pre_request(method, url, headers)

    def _run_post_request_hooks(self, method, url, headers, response):
        for callback in self.callbacks:
            callback.post_request(method, url, headers, response)


def test_http():
    """
    Runs a few tests on error diagnosis.

    This test takes a while to run, and is not used in the automated tests
    during building.  Run the test by hand to exercise the code.  Be sure
    to run in both Python 2 and Python 3.
    """

    from .exception import BadJson

    b2_http = B2Http()

    # Error from B2
    print('TEST: error object from B2')
    try:
        b2_http.post_json_return_json(
            'https://api.backblazeb2.com/b2api/v1/b2_get_file_info', {}, {}
        )
        assert False, 'should have failed with bad json'
    except BadJson as e:
        assert str(e) == 'Bad request: required field fileId is missing'

    # Successful get
    print('TEST: get')
    with b2_http.get_content(
        'https://api.backblazeb2.com/test/echo_zeros?length=10', {}
    ) as response:
        assert response.status_code == 200
        response_data = six.b('').join(response.iter_content())
        assert response_data == six.b(chr(0) * 10)

    # Successful post
    print('TEST: post')
    response_dict = b2_http.post_json_return_json(
        'https://api.backblazeb2.com/api/build_version', {}, {}
    )
    assert 'timestamp' in response_dict

    # Unknown host
    print('TEST: unknown host')
    try:
        b2_http.post_json_return_json('https://unknown.backblazeb2.com', {}, {})
        assert False, 'should have failed with unknown host'
    except UnknownHost:
        pass

    # Broken pipe
    print('TEST: broken pipe')
    try:
        data = six.BytesIO(six.b(chr(0)) * 10000000)
        b2_http.post_content_return_json('https://api.backblazeb2.com/bad_url', {}, data)
        assert False, 'should have failed with broken pipe'
    except BrokenPipe:
        pass

    # Generic connection error
    print('TEST: generic connection error')
    try:
        with b2_http.get_content('https://www.backblazeb2.com:80/bad_url', {}) as response:
            assert False, 'should have failed with connection error'
            response.iter_content()  # make pyflakes happy
    except B2ConnectionError:
        pass
