######################################################################
#
# File: test/unit/simulator_hooks.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Test hooks that work both on the in-process ``RawSimulator`` and on b2-simulator.

On b2-simulator (the default) each hook uses b2-simulator's control plane (``docs/CONTRACT.md``
in backblaze-labs/b2-simulator), so the SDK meets the condition over real HTTP. In a test marked
``rawsimulator``, or with ``--rawsimulator``, it uses ``RawSimulator``'s own test methods.
"""

from __future__ import annotations

import contextlib
import hashlib
import itertools
import time
from unittest import mock

from b2sdk._internal.exception import B2RequestTimeout, interpret_b2_error
from test.unit import b2_simulator

UPLOAD_ENDPOINT = 'b2_upload_file'
DOWNLOAD_ENDPOINTS = ('b2_download_file_by_id', 'b2_download_file_by_name')
# b2-simulator's default authTokenTtlMs (24 h); the harness never changes it.
AUTH_TOKEN_TTL_MS = 24 * 60 * 60 * 1000
# On b2-simulator an upload times out after this many seconds, and the server holds the
# request for STALL_MS before it answers.
UPLOAD_TIMEOUT_SECONDS = 0.5
STALL_MS = 1500
WAIT_SECONDS = 10


def fail_uploads(raw_api, status: int, code: str, count: int = 1):
    """
    Make the next ``count`` ``b2_upload_file`` calls fail with the B2 error ``status`` / ``code``.

    On ``RawSimulator`` the upload raises the exception that the SDK's HTTP layer raises for that
    answer (``interpret_b2_error``), so both runs give the SDK the same error.
    """
    if b2_simulator.enabled():
        b2_simulator.control(
            'POST',
            '/faults',
            {'on': UPLOAD_ENDPOINT, 'status': status, 'code': code, 'count': count},
        )
    else:
        raw_api.set_upload_errors(
            [interpret_b2_error(status, code, f'injected {code}', {}) for _ in range(count)]
        )


def assert_upload_faults_fired(raw_api, count: int):
    """
    Check that exactly ``count`` uploads failed on purpose, so a test that recovers from them
    cannot pass without meeting them.
    """
    if b2_simulator.enabled():
        fired = [
            entry
            for entry in b2_simulator.control('GET', '/journal')['entries']
            if entry['endpoint'] == UPLOAD_ENDPOINT and entry['fault'] is not None
        ]
        assert len(fired) == count, fired
    else:
        assert raw_api.upload_errors == [], 'an injected upload error was not used'


@contextlib.contextmanager
def upload_times_out_once(raw_api):
    """
    Make the next ``b2_upload_file`` call time out.

    On b2-simulator the server holds the request (a ``stall`` wire fault) longer than the client
    waits for it, so ``requests`` times out and the SDK raises ``B2RequestTimeout``. On exit the
    context waits until the held request has finished, so it cannot reach the next test.
    On ``RawSimulator`` the upload raises ``B2RequestTimeout``, as the HTTP layer would.
    """
    if not b2_simulator.enabled():
        raw_api.set_upload_errors([B2RequestTimeout('injected upload timeout')])
        yield
        return
    b2_simulator.control(
        'POST', '/wire-faults', {'on': UPLOAD_ENDPOINT, 'kind': 'stall', 'ms': STALL_MS, 'count': 1}
    )
    with mock.patch.object(raw_api.b2_http, 'TIMEOUT_FOR_UPLOAD', UPLOAD_TIMEOUT_SECONDS):
        yield
    _wait_for(
        lambda: any(
            e['fault'] == 'stall' for e in b2_simulator.control('GET', '/journal')['entries']
        ),
        'the stalled upload to finish',
    )


def expire_account_token(raw_api, account_auth_token: str):
    """
    Expire an account authorization token.

    b2-simulator has no route that expires one account token, so the virtual clock moves past
    the token lifetime: every token issued so far expires, as it would on B2 after 24 hours.
    """
    if b2_simulator.enabled():
        b2_simulator.control('POST', '/clock', {'advanceMs': AUTH_TOKEN_TTL_MS + 1})
    else:
        raw_api.expire_auth_token(account_auth_token)


def truncate_downloads():
    """
    Make every download answer end early, so the SDK has to fetch the rest again.

    On b2-simulator the server sends the headers and half the body, then drops the connection
    (``reset-mid-response``). ``RawSimulator`` cannot do this per test: use a ``RawSimulator``
    subclass whose responses are short instead.
    """
    for endpoint in DOWNLOAD_ENDPOINTS:
        b2_simulator.control('POST', '/wire-faults', {'on': endpoint, 'kind': 'reset-mid-response'})


def truncated_downloads() -> int:
    """How many download answers b2-simulator has cut short since the test began."""
    return sum(
        1
        for entry in b2_simulator.control('GET', '/journal')['entries']
        if entry['endpoint'] in DOWNLOAD_ENDPOINTS and entry['fault'] == 'reset-mid-response'
    )


def corrupt_stored_sha1(raw_api, bucket_id: str, file_id: str, content_sha1: str = 'f' * 40):
    """Make a stored file report a different SHA-1 while its bytes stay the same."""
    if b2_simulator.enabled():
        b2_simulator.control(
            'POST', '/files/content-sha1', {'fileId': file_id, 'contentSha1': content_sha1}
        )
    else:
        raw_api.bucket_id_to_bucket[bucket_id].file_id_to_file[file_id].content_sha1 = content_sha1


def upload_local_file_with_unverified_sha1(raw_api, bucket, path, file_name):
    """
    Upload a local file whose stored SHA-1 is marked ``unverified:``.

    B2 stores a SHA-1 sent with that prefix as is (b2-simulator too). ``RawSimulator`` refuses the
    prefix on upload, so there the stored SHA-1 is changed after a normal upload.
    """
    with open(path, 'rb') as f:
        content_sha1 = hashlib.sha1(f.read()).hexdigest()
    if b2_simulator.enabled():
        return bucket.upload_local_file(path, file_name, sha1_sum='unverified:' + content_sha1)
    file_version = bucket.upload_local_file(path, file_name)
    stored = raw_api.bucket_id_to_bucket[bucket.id_].file_id_to_file[file_version.id_]
    stored.content_sha1 = 'unverified:' + content_sha1
    return file_version


def restrict_to_name_prefix(api, bucket_id: str, name_prefix: str):
    """
    Make ``api`` use a key restricted to ``name_prefix`` in the bucket ``bucket_id``.

    On b2-simulator a new key restricted to the bucket and the prefix is created and authorized,
    as B2 allows a name prefix only together with a bucket. ``RawSimulator`` refuses a
    bucket-restricted key in ``b2_list_unfinished_large_files`` whatever the bucket, so there the
    authorized key gets the prefix alone.
    """
    if not b2_simulator.enabled():
        key_id = api.account_info.get_application_key_id()
        api.session.raw_api.key_id_to_key[key_id].name_prefix_or_none = name_prefix
        return
    from test.unit.test_base import create_key

    key = create_key(
        api,
        ['listBuckets', 'listFiles', 'readFiles', 'writeFiles'],
        'name-prefix-key',
        bucket_id=bucket_id,
        name_prefix=name_prefix,
    )
    api.authorize_account(
        application_key_id=key.id_, application_key=key.application_key, realm='production'
    )
    assert api.account_info.get_allowed()['namePrefix'] == name_prefix


def suspend_notification_rule(raw_api, bucket_id: str, rule_name: str, reason: str):
    """Suspend an event notification rule, as Backblaze does."""
    if b2_simulator.enabled():
        b2_simulator.control(
            'POST',
            '/notification-rules/suspend',
            {'bucketId': bucket_id, 'name': rule_name, 'suspensionReason': reason},
        )
    else:
        raw_api.bucket_id_to_bucket[bucket_id].simulate_notification_rule_suspension(
            rule_name, reason
        )


def allow_unlimited_upload_urls(raw_api, bucket_id: str):
    """Lift ``RawSimulator``'s limit of 200 upload URLs per bucket; b2-simulator has none."""
    if not b2_simulator.enabled():
        raw_api.bucket_id_to_bucket[bucket_id].upload_url_counter = itertools.count()


def _wait_for(condition, what):
    deadline = time.monotonic() + WAIT_SECONDS
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f'timed out waiting for {what}')
        time.sleep(0.05)
