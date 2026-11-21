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
reach the SDK's DEBUG trace or the text of an exception raised by the call.

There is one test per (surface, path); each is run once per sensitive input
(``signing_secret``, ``custom_header``) and asserts exactly that cell:

* surfaces: SDK DEBUG trace (every ``logging`` record, with tracebacks, at DEBUG level; the SDK
  traces little on the success, pre-authorization and validation paths, so those tests also
  emit a marker record to prove the capture is live; on the API error path the SDK's own
  records, including the HTTP error, are asserted to be present) and
  exception text (message, repr and formatted traceback of whatever the call raised);
* paths: success (simulator-backed), failure before authorization, API error (a real
  ``B2RawHTTPApi`` over a faked HTTP session answering 400), and a local validation error
  (a request that cannot be serialized, raised before anything is sent).

On the success path nothing is raised, so the success x exception test can only assert that the
call does not raise and that no exception text can carry the secret. It is weaker than the
failure-path exception tests and is documented as such rather than dressed up.
"""

from __future__ import annotations

import json
import logging
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

from b2sdk._internal.b2http import B2Http

SECRET_VALUES = {
    'signing_secret': 'hmac-signing-secret-must-not-leak-4b1e',
    'custom_header': 'custom-header-value-must-not-leak-9d7c',
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
    api = B2Api(InMemoryAccountInfo(), api_config=B2HttpApiConfig(_raw_api_class=RawSimulator))
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
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_success_path(
    bucket, caplog, sensitive_input
):
    with caplog.at_level(logging.DEBUG):
        logging.getLogger('b2sdk').debug(CAPTURE_MARKER)
        bucket.set_notification_rules(sensitive_rules())
    sdk_debug_trace = caplog.text
    assert CAPTURE_MARKER in sdk_debug_trace  # DEBUG capture works
    assert SECRET_VALUES[sensitive_input] not in sdk_debug_trace


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_pre_authorization_path(
    unauthorized_bucket, caplog, sensitive_input
):
    with caplog.at_level(logging.DEBUG):
        logging.getLogger('b2sdk').debug(CAPTURE_MARKER)
        with pytest.raises(Exception):
            unauthorized_bucket.set_notification_rules(sensitive_rules())
    sdk_debug_trace = caplog.text
    assert CAPTURE_MARKER in sdk_debug_trace  # DEBUG capture works
    assert SECRET_VALUES[sensitive_input] not in sdk_debug_trace


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_api_error_path(
    http_rejecting_bucket, caplog, sensitive_input
):
    with caplog.at_level(logging.DEBUG), pytest.raises(Exception):
        http_rejecting_bucket.set_notification_rules(sensitive_rules())
    sdk_debug_trace = caplog.text
    assert 'interpret_b2_error' in sdk_debug_trace  # the SDK traced the service error itself
    assert SECRET_VALUES[sensitive_input] not in sdk_debug_trace


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_sdk_debug_trace_on_validation_error_path(
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
def test_sensitive_input_stays_out_of_exception_on_success_path(bucket, sensitive_input):
    # Weaker than the failure-path cells: a successful call raises nothing, so there is no
    # exception text that could carry the secret. This pins that the call really does succeed
    # (nothing raised) and that the exception text is therefore free of the secret.
    raised = None
    try:
        bucket.set_notification_rules(sensitive_rules())
    except Exception as error:
        raised = error
    exception_text_of_call = '' if raised is None else exception_text(raised)
    assert raised is None
    assert SECRET_VALUES[sensitive_input] not in exception_text_of_call


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_exception_on_pre_authorization_path(
    unauthorized_bucket, sensitive_input
):
    with pytest.raises(Exception) as excinfo:
        unauthorized_bucket.set_notification_rules(sensitive_rules())
    assert SECRET_VALUES[sensitive_input] not in exception_text(excinfo.value)


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_exception_on_api_error_path(
    http_rejecting_bucket, sensitive_input
):
    with pytest.raises(Exception) as excinfo:
        http_rejecting_bucket.set_notification_rules(sensitive_rules())
    assert SECRET_VALUES[sensitive_input] not in exception_text(excinfo.value)
    assert 'rejected by the service' in exception_text(excinfo.value)  # the service error is real


@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_sensitive_input_stays_out_of_exception_on_validation_error_path(
    http_rejecting_bucket, sensitive_input
):
    with pytest.raises(TypeError) as excinfo:
        http_rejecting_bucket.set_notification_rules(unserializable_rules())
    assert SECRET_VALUES[sensitive_input] not in exception_text(excinfo.value)
