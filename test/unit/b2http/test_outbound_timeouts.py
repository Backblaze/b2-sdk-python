######################################################################
#
# File: test/unit/b2http/test_outbound_timeouts.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Every outbound HTTP entry point of ``B2Http`` must stop waiting for an unresponsive
server within its configured timeout and surface ``B2RequestTimeout`` (a controlled
failure) instead of hanging the caller.

The server here is a real local TCP listener that accepts connections and never
answers, so the timeout is exercised by the actual ``requests`` socket deadline.
No external network and no B2 credentials are involved.
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time

import pytest
import requests
from apiver_deps_exception import B2RequestTimeout

from b2sdk._internal.api_config import B2HttpApiConfig
from b2sdk._internal.b2http import B2Http

# Distinct values, so each entry point can be checked against the setting it should use.
CONNECTION_TIMEOUT_SECONDS = 0.2
TIMEOUT_SECONDS = 0.3
TIMEOUT_FOR_COPY_SECONDS = 0.4
TIMEOUT_FOR_UPLOAD_SECONDS = 0.5
# Generous ceiling that still proves the call was bounded by the timeout, not by luck.
MAX_ELAPSED_SECONDS = 10.0

# A regression that removes a timeout must fail the test rather than hang the suite.
pytestmark = pytest.mark.timeout(30)


class ShortTimeoutB2Http(B2Http):
    """B2Http with every class-level timeout shrunk so a hung server is detected quickly."""

    CONNECTION_TIMEOUT = CONNECTION_TIMEOUT_SECONDS
    TIMEOUT = TIMEOUT_SECONDS
    TIMEOUT_FOR_COPY = TIMEOUT_FOR_COPY_SECONDS
    TIMEOUT_FOR_UPLOAD = TIMEOUT_FOR_UPLOAD_SECONDS


@pytest.fixture
def unresponsive_server_url(monkeypatch):
    """URL of a local server that accepts connections but never sends a response."""
    monkeypatch.setenv('NO_PROXY', '127.0.0.1')
    monkeypatch.setenv('no_proxy', '127.0.0.1')
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('127.0.0.1', 0))
    listener.listen(8)
    listener.settimeout(0.1)
    stop = threading.Event()
    held = []

    def accept_and_hold():
        while not stop.is_set():
            with contextlib.suppress(OSError):
                held.append(listener.accept()[0])

    thread = threading.Thread(target=accept_and_hold, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{listener.getsockname()[1]}/b2api/v3/b2_list_buckets'
    finally:
        stop.set()
        thread.join(timeout=5)
        for connection in held:
            connection.close()
        listener.close()


@pytest.fixture
def b2_http():
    http = ShortTimeoutB2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    # Record the ``timeout`` each call hands to the real session; the request still goes out.
    http.recorded_timeouts = []
    real_request = http.session.request

    def recording_request(*args, **kwargs):
        http.recorded_timeouts.append(kwargs.get('timeout'))
        return real_request(*args, **kwargs)

    http.session.request = recording_request
    try:
        yield http
    finally:
        http.session.close()


def _request(http, url):
    return http.request('GET', url, headers={}, try_count=1)


def _post_content_return_json(http, url):
    return http.post_content_return_json(url, {}, b'{}', try_count=1)


def _post_json_return_json(http, url):
    return http.post_json_return_json(url, {}, {'accountId': 'a'}, try_count=1)


def _get_content(http, url):
    return http.get_content(url, {}, try_count=1)


def _head_content(http, url):
    return http.head_content(url, {}, try_count=1)


@pytest.mark.parametrize(
    'call, read_timeout',
    [
        pytest.param(_request, TIMEOUT_FOR_UPLOAD_SECONDS, id='request'),
        pytest.param(
            _post_content_return_json, TIMEOUT_FOR_UPLOAD_SECONDS, id='post_content_return_json'
        ),
        pytest.param(_post_json_return_json, TIMEOUT_FOR_COPY_SECONDS, id='post_json_return_json'),
        pytest.param(_get_content, TIMEOUT_SECONDS, id='get_content'),
        pytest.param(_head_content, TIMEOUT_FOR_UPLOAD_SECONDS, id='head_content'),
    ],
)
def test_unresponsive_server_raises_request_timeout_within_bound(
    call, read_timeout, b2_http, unresponsive_server_url
):
    started = time.monotonic()
    # The tuple only admits the raw requests timeout so that the stricter isinstance
    # check below reports a leaked, untranslated timeout as its own failure.
    with pytest.raises((B2RequestTimeout, requests.Timeout)) as raised:
        call(b2_http, unresponsive_server_url)
    elapsed = time.monotonic() - started

    assert isinstance(raised.value, B2RequestTimeout), 'timeout was not translated to a B2Error'
    assert b2_http.recorded_timeouts == [(CONNECTION_TIMEOUT_SECONDS, read_timeout)]
    assert elapsed >= read_timeout * 0.5, 'call failed before the timeout could have expired'
    assert elapsed < MAX_ELAPSED_SECONDS, f'call was not bounded by its timeout: {elapsed:.1f}s'


def test_explicit_request_timeout_overrides_default_bound(b2_http, unresponsive_server_url):
    """A caller-supplied ``_timeout`` is what bounds the call, not the larger class default."""
    b2_http.TIMEOUT_FOR_UPLOAD = 3600
    started = time.monotonic()
    with pytest.raises(B2RequestTimeout):
        b2_http.request('GET', unresponsive_server_url, headers={}, try_count=1, _timeout=0.3)
    assert b2_http.recorded_timeouts == [(CONNECTION_TIMEOUT_SECONDS, 0.3)]
    assert time.monotonic() - started < MAX_ELAPSED_SECONDS
