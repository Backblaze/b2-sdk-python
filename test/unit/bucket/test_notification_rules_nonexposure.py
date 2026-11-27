######################################################################
#
# File: test/unit/bucket/test_notification_rules_nonexposure.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Operation-level secret non-exposure for ``Bucket.set_notification_rules``.

A notification rule can carry an HMAC signing secret and a custom header value. Neither may
reach the SDK's DEBUG trace or the text of an exception raised by the call. Each test is run
once per sensitive input (``signing_secret``, ``custom_header``).

What each case can and cannot detect:

* API error path (``B2RawHTTPApi`` over a real ``B2Http``, only ``session.request`` faked to
  answer 400): detects a leak anywhere in the SDK stack, including ``b2http`` and the raw API
  (for example logging or raising with the request body). The debug test also requires the
  SDK's own ``interpret_b2_error`` call record to be present, so it cannot pass on an empty
  trace. It overlaps ``test_exception.py::test_http_error_trace_keeps_request_values_private``
  for log messages, but also covers formatted tracebacks (``caplog.text``); the exception test
  is the only one asserting on exception text.
* Pre-authorization path and unserializable-request path: detect leaks in ``Bucket`` and
  ``Session`` on a failure path (the pre-authorization case runs on ``RawSimulator``, so it
  cannot see anything below ``Session``). The SDK logs almost nothing there, so the debug tests
  emit a marker record to prove the capture is live; the marker does not prove the SDK
  logging itself.
* Success path: not covered here. ``test_notification_rule_tracing.py`` already asserts that
  ``set_notification_rules`` does not trace the secret on success, and a successful call
  raises nothing, so an exception-text test there cannot fail.
"""

from __future__ import annotations

import json
import logging
import secrets
import traceback

import pytest
import requests
from apiver_deps import (
    B2Api,
    B2HttpApiConfig,
    B2RawHTTPApi,
    InMemoryAccountInfo,
    RawSimulator,
)
from apiver_deps_exception import BadRequest, MissingAccountData

from b2sdk._internal.b2http import B2Http
from test.unit.b2_simulator import raw_api_class

# Random per run, so no credential-shaped literal lives in the source.
SECRET_VALUES = {
    'signing_secret': secrets.token_hex(16),
    'custom_header': secrets.token_urlsafe(24),
}


def sensitive_rules():
    """One webhook rule carrying both sensitive inputs."""
    return [
        {
            'eventTypes': ['b2:ObjectCreated:*'],
            'isEnabled': True,
            'name': 'sensitive-webhook',
            'objectNamePrefix': '',
            'targetConfiguration': {
                'targetType': 'webhook',
                'url': 'https://example.com/webhook',
                'hmacSha256SigningSecret': SECRET_VALUES['signing_secret'],
                'customHeaders': [
                    {'name': 'X-Webhook-Token', 'value': SECRET_VALUES['custom_header']}
                ],
            },
        }
    ]


def unserializable_rules():
    """Sensitive rules that cannot be sent: ``eventTypes`` is a ``set``, which JSON rejects."""
    rules = sensitive_rules()
    rules[0]['eventTypes'] = {'b2:ObjectCreated:*'}
    return rules


def exception_text(error: BaseException) -> str:
    """Message, repr and formatted traceback (including chained causes) of an exception."""
    formatted = traceback.format_exception(type(error), error, error.__traceback__)
    return '\n'.join([str(error), repr(error), *formatted])


@pytest.fixture
def http_rejecting_bucket(b2api, mocker):
    """
    A bucket created through the simulator, whose API object then switches to the real HTTP
    raw API talking to a faked HTTP session that answers every request with a 400
    ``bad_request``.
    """
    bucket = b2api.create_bucket('test-bucket', 'allPrivate')

    def respond(method, url, **kwargs):
        response = requests.Response()
        response.url = url
        response.status_code = 400
        response._content = json.dumps(
            {'status': 400, 'code': 'bad_request', 'message': 'rejected by the service'}
        ).encode()
        return response

    http = B2Http(B2HttpApiConfig(install_clock_skew_hook=False))
    mocker.patch.object(http.session, 'request', side_effect=respond)
    b2api.session.raw_api = B2RawHTTPApi(http)
    return bucket


@pytest.fixture
def unauthorized_bucket():
    """A bucket whose account info was cleared, so the call fails before it is authorized."""
    api = B2Api(
        InMemoryAccountInfo(),
        api_config=B2HttpApiConfig(_raw_api_class=raw_api_class(RawSimulator)),
    )
    account_id, master_key = api.session.raw_api.create_account()
    api.authorize_account(
        application_key_id=account_id, application_key=master_key, realm='production'
    )
    bucket = api.create_bucket('test-bucket', 'allPrivate')
    api.account_info.clear()
    return bucket


CAPTURE_MARKER = 'debug capture is active'

# ── SDK debug trace ──────────────────────────────────────────────────────────


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_pre_authorization_path(
    unauthorized_bucket, caplog, sensitive_input
):
    with caplog.at_level(logging.DEBUG):
        logging.getLogger('b2sdk').debug(CAPTURE_MARKER)
        with pytest.raises(MissingAccountData):
            unauthorized_bucket.set_notification_rules(sensitive_rules())
    sdk_debug_trace = caplog.text
    assert CAPTURE_MARKER in sdk_debug_trace  # DEBUG capture works
    assert SECRET_VALUES[sensitive_input] not in sdk_debug_trace


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_api_error_path(
    http_rejecting_bucket, caplog, sensitive_input
):
    with caplog.at_level(logging.DEBUG), pytest.raises(BadRequest):
        http_rejecting_bucket.set_notification_rules(sensitive_rules())
    sdk_debug_trace = caplog.text
    # the SDK's own trace record of the service error is present (stable logfury message)
    assert any(
        'calling interpret_b2_error(' in record.getMessage()
        and 'status=400' in record.getMessage()
        and "code='bad_request'" in record.getMessage()
        for record in caplog.records
    )
    assert SECRET_VALUES[sensitive_input] not in sdk_debug_trace


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_unserializable_request_path(
    http_rejecting_bucket, caplog, sensitive_input
):
    with caplog.at_level(logging.DEBUG):
        logging.getLogger('b2sdk').debug(CAPTURE_MARKER)
        with pytest.raises(TypeError):
            http_rejecting_bucket.set_notification_rules(unserializable_rules())
    sdk_debug_trace = caplog.text
    assert CAPTURE_MARKER in sdk_debug_trace  # DEBUG capture works
    assert SECRET_VALUES[sensitive_input] not in sdk_debug_trace


# ── exception text ───────────────────────────────────────────────────────────


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_exception_on_pre_authorization_path(
    unauthorized_bucket, sensitive_input
):
    with pytest.raises(MissingAccountData) as excinfo:
        unauthorized_bucket.set_notification_rules(sensitive_rules())
    assert SECRET_VALUES[sensitive_input] not in exception_text(excinfo.value)


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_exception_on_api_error_path(
    http_rejecting_bucket, sensitive_input
):
    with pytest.raises(BadRequest) as excinfo:
        http_rejecting_bucket.set_notification_rules(sensitive_rules())
    assert SECRET_VALUES[sensitive_input] not in exception_text(excinfo.value)
    assert 'rejected by the service' in exception_text(excinfo.value)  # the service error is real


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_exception_on_unserializable_request_path(
    http_rejecting_bucket, sensitive_input
):
    with pytest.raises(TypeError) as excinfo:
        http_rejecting_bucket.set_notification_rules(unserializable_rules())
    assert SECRET_VALUES[sensitive_input] not in exception_text(excinfo.value)
