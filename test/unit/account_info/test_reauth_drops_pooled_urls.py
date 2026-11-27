######################################################################
#
# File: test/unit/account_info/test_reauth_drops_pooled_urls.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
import io
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from apiver_deps import ALL_CAPABILITIES, B2HttpApiConfig, B2Session, RawSimulator
from apiver_deps_exception import InvalidAuthToken, MissingAccountData

from test.unit import simulator_hooks
from test.unit.b2_simulator import raw_api_class

from .fixtures import *  # noqa: F401, F403


@pytest.fixture
def session_with_keys(account_info):
    session = B2Session(
        account_info=account_info,
        api_config=B2HttpApiConfig(_raw_api_class=raw_api_class(RawSimulator)),
    )
    simulator = session.raw_api
    key_id, master_key = simulator.create_account()
    session.authorize_account('production', key_id, master_key)
    account_id = account_info.get_account_id()  # B2 account ids differ from key ids
    bucket_id = session.create_bucket(account_id, 'reauth-test', 'allPrivate')['bucketId']
    file_id = session.start_large_file(bucket_id, 'large.bin', 'application/octet-stream', {})[
        'fileId'
    ]
    keys = [
        simulator.create_key(
            account_info.get_api_url(),
            account_info.get_account_auth_token(),
            account_id,
            ALL_CAPABILITIES,
            name,
            None,
            None,
            None,
        )
        for name in ('key-a', 'key-b')
    ]
    session.authorize_account('production', keys[0]['applicationKeyId'], keys[0]['applicationKey'])
    return session, keys, bucket_id, file_id


@pytest.fixture(params=['bucket', 'part'])
def pooled_uploads(session_with_keys, request):
    session, _, bucket_id, file_id = session_with_keys
    info = session.account_info
    if request.param == 'bucket':
        resource_id = bucket_id
        get_url = session.get_upload_url
        put = info.put_bucket_upload_url
        take = info.take_bucket_upload_url
    else:
        resource_id = file_id
        get_url = session.get_upload_part_url
        put = info.put_large_file_upload_url
        take = info.take_large_file_upload_url

    responses = [get_url(resource_id) for _ in range(2)]
    pairs = [(response['uploadUrl'], response['authorizationToken']) for response in responses]
    for pair in pairs:
        put(resource_id, *pair)
    assert set(take(resource_id) for _ in pairs) == set(pairs), 'pool was not populated'
    for pair in pairs:
        put(resource_id, *pair)
    return resource_id, take, pairs


def test_different_key_drops_pooled_upload_urls(session_with_keys, pooled_uploads):
    session, keys, _, _ = session_with_keys
    resource_id, take, pairs = pooled_uploads
    account_id = session.account_info.get_account_id()

    session.authorize_account('production', keys[1]['applicationKeyId'], keys[1]['applicationKey'])

    assert session.account_info.get_account_id() == account_id
    assert session.account_info.get_application_key_id() == keys[1]['applicationKeyId']
    pool_is_empty = all(take(resource_id) == (None, None) for _ in pairs)
    assert pool_is_empty, 'upload data from the previous authorization remained pooled'


def test_same_key_refresh_preserves_pooled_upload_urls(session_with_keys, pooled_uploads):
    session, keys, _, _ = session_with_keys
    resource_id, take, pairs = pooled_uploads

    session.authorize_account('production', keys[0]['applicationKeyId'], keys[0]['applicationKey'])

    assert set(take(resource_id) for _ in pairs) == set(pairs)


def test_failed_authorization_preserves_pooled_upload_urls(session_with_keys, pooled_uploads):
    session, keys, _, _ = session_with_keys
    resource_id, take, pairs = pooled_uploads

    with pytest.raises(InvalidAuthToken) as caught:
        session.authorize_account('production', keys[1]['applicationKeyId'], 'invalid-test-key')

    assert caught.value.code == 'unauthorized'
    assert session.account_info.get_application_key_id() == keys[0]['applicationKeyId']
    assert set(take(resource_id) for _ in pairs) == set(pairs)


@pytest.mark.parametrize('use_pooled_url', [False, True], ids=['new-url', 'pooled-url'])
def test_upload_finishing_after_reauthorization_cannot_refill_new_pool(
    session_with_keys, pooled_uploads, use_pooled_url, mocker
):
    session, keys, bucket_id, file_id = session_with_keys
    resource_id, take, pairs = pooled_uploads
    if not use_pooled_url:
        for _ in pairs:
            take(resource_id)

    # Simulator-sized data exercises both upload paths without live B2 requests.
    data = b'x' * session.raw_api.MIN_PART_SIZE
    digest = hashlib.sha1(data).hexdigest()
    started, release = threading.Event(), threading.Event()
    sent = []
    if resource_id == bucket_id:
        upload_method = 'upload_file'
        get_url_method = 'get_upload_url'

        def upload():
            return session.upload_file(
                bucket_id,
                'small.bin',
                len(data),
                'application/octet-stream',
                digest,
                {},
                io.BytesIO(data),
            )

    else:
        upload_method = 'upload_part'
        get_url_method = 'get_upload_part_url'

        def upload():
            return session.upload_part(file_id, 1, len(data), digest, io.BytesIO(data))

    original = getattr(session.raw_api, upload_method)

    def blocked_upload(*args, **kwargs):
        sent.append(args[:2])
        if len(sent) == 1:
            started.set()
            assert release.wait(10), 'authorization did not release the upload'
        return original(*args, **kwargs)

    issued = mocker.spy(session.raw_api, get_url_method)
    mocker.patch.object(session.raw_api, upload_method, side_effect=blocked_upload)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(upload)
        try:
            assert started.wait(10), 'upload did not start'
            session.authorize_account(
                'production', keys[1]['applicationKeyId'], keys[1]['applicationKey']
            )
        finally:
            release.set()
        pending.result(timeout=10)

    pool_is_empty = all(take(resource_id) == (None, None) for _ in pairs)
    assert pool_is_empty, 'the completed upload refilled the pool after authorization changed'

    upload()
    assert issued.call_args.args[1] == session.account_info.get_account_auth_token()
    assert sent[1] == (issued.spy_return['uploadUrl'], issued.spy_return['authorizationToken'])
    assert sent[1] != sent[0]
    assert take(resource_id) == sent[1]


@pytest.mark.parametrize(
    'change_key', [False, True], ids=['current-generation', 'retired-generation']
)
def test_tagged_upload_url_returns_respect_generation(
    session_with_keys, pooled_uploads, change_key
):
    session, keys, bucket_id, _ = session_with_keys
    resource_id, take, pairs = pooled_uploads
    info = session.account_info
    large_file = resource_id != bucket_id
    upload_url, token, generation = info._take_upload_url_tagged(resource_id, large_file=large_file)
    assert (upload_url, token) in pairs
    if change_key:
        session.authorize_account(
            'production', keys[1]['applicationKeyId'], keys[1]['applicationKey']
        )
        assert info._get_upload_pool_generation() > generation

    info._put_upload_url_tagged(resource_id, upload_url, token, generation, large_file=large_file)
    returned = take(resource_id)
    expected = (None, None) if change_key else (upload_url, token)
    matches_generation = returned == expected
    assert matches_generation, 'a tagged return was pooled in the wrong generation'


def test_reauthorization_before_acquisition_keeps_the_current_url(
    session_with_keys, pooled_uploads, mocker
):
    session, keys, bucket_id, file_id = session_with_keys
    resource_id, take, _ = pooled_uploads
    is_bucket = resource_id == bucket_id
    acquire_name = '_get_upload_data' if is_bucket else '_get_upload_part_data'
    original = getattr(session, acquire_name)

    def reauthorize_then_acquire(*args, **kwargs):
        session.authorize_account(
            'production', keys[1]['applicationKeyId'], keys[1]['applicationKey']
        )
        return original(*args, **kwargs)

    mocker.patch.object(session, acquire_name, side_effect=reauthorize_then_acquire)
    issued = mocker.spy(session.raw_api, 'get_upload_url' if is_bucket else 'get_upload_part_url')
    # Use the simulator's part size to exercise the request without live B2 I/O.
    data = b'x' * session.raw_api.MIN_PART_SIZE
    digest = hashlib.sha1(data).hexdigest()
    if is_bucket:
        session.upload_file(
            bucket_id,
            'current.bin',
            len(data),
            'application/octet-stream',
            digest,
            {},
            io.BytesIO(data),
        )
    else:
        session.upload_part(file_id, 1, len(data), digest, io.BytesIO(data))

    assert issued.call_args.args[1] == session.account_info.get_account_auth_token()
    current_url_was_pooled = take(resource_id) == (
        issued.spy_return['uploadUrl'],
        issued.spy_return['authorizationToken'],
    )
    assert current_url_was_pooled, 'a URL acquired after reauthorization was discarded'


def test_take_is_serialized_with_pool_reset(session_with_keys, pooled_uploads, mocker):
    session, keys, bucket_id, _ = session_with_keys
    resource_id, take, pairs = pooled_uploads
    info = session.account_info
    old_pool = info._bucket_uploads if resource_id == bucket_id else info._large_file_uploads
    real_lock = info._auth_data_lock
    local = threading.local()
    authorizer = []
    entered, release, authorization_progress = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    order = []

    class ObservedLock:
        def __enter__(self):
            if not real_lock.acquire(blocking=False):
                if authorizer and threading.get_ident() == authorizer[0]:
                    authorization_progress.set()
                real_lock.acquire()
            local.depth = getattr(local, 'depth', 0) + 1
            return self

        def __exit__(self, *args):
            local.depth -= 1
            real_lock.release()
            if authorizer and threading.get_ident() == authorizer[0] and local.depth == 0:
                authorization_progress.set()

    original_take = old_pool.take
    original_reset = info._reset_upload_pools

    def blocked_take(key):
        entered.set()
        assert release.wait(10), 'take was not released'
        result = original_take(key)
        order.append('take')
        return result

    def recorded_reset():
        original_reset()
        order.append('reset')

    def authorize():
        authorizer.append(threading.get_ident())
        session.authorize_account(
            'production', keys[1]['applicationKeyId'], keys[1]['applicationKey']
        )

    mocker.patch.object(info, '_auth_data_lock', ObservedLock())
    mocker.patch.object(old_pool, 'take', side_effect=blocked_take)
    mocker.patch.object(info, '_reset_upload_pools', side_effect=recorded_reset)
    with ThreadPoolExecutor(max_workers=2) as executor:
        pending_take = executor.submit(take, resource_id)
        try:
            assert entered.wait(10), 'take did not reach the pool'
            pending_auth = executor.submit(authorize)
            assert authorization_progress.wait(
                10
            ), 'authorization did not reach its critical section'
        finally:
            release.set()
        assert pending_take.result(timeout=10) in pairs
        pending_auth.result(timeout=10)

    assert order == ['take', 'reset'], 'reauthorization reset a pool while a take was in progress'
    assert take(resource_id) == (None, None)


def _upload(session, bucket_id, file_id, *, large_file):
    data = b'x' * session.raw_api.MIN_PART_SIZE
    digest = hashlib.sha1(data).hexdigest()
    if large_file:
        return session.upload_part(file_id, 1, len(data), digest, io.BytesIO(data))
    return session.upload_file(
        bucket_id,
        'generation.bin',
        len(data),
        'application/octet-stream',
        digest,
        {},
        io.BytesIO(data),
    )


def test_url_request_in_flight_does_not_block_reauthorization_or_refill_new_pool(
    session_with_keys, pooled_uploads, mocker
):
    session, keys, bucket_id, file_id = session_with_keys
    resource_id, take, pairs = pooled_uploads
    large_file = resource_id == file_id
    for _ in pairs:
        take(resource_id)
    method = 'get_upload_part_url' if large_file else 'get_upload_url'
    original = getattr(session.raw_api, method)
    entered, release = threading.Event(), threading.Event()

    def blocked_get(*args, **kwargs):
        entered.set()
        assert release.wait(10), 'URL request was not released'
        return original(*args, **kwargs)

    mocker.patch.object(session.raw_api, method, side_effect=blocked_get)
    with ThreadPoolExecutor(max_workers=2) as executor:
        pending_upload = executor.submit(
            _upload, session, bucket_id, file_id, large_file=large_file
        )
        try:
            assert entered.wait(10), 'URL request did not start'
            authorization = executor.submit(
                session.authorize_account,
                'production',
                keys[1]['applicationKeyId'],
                keys[1]['applicationKey'],
            )
            authorization.result(timeout=10)
        finally:
            release.set()
        pending_upload.result(timeout=10)
    pool_is_empty = take(resource_id) == (None, None)
    assert pool_is_empty, 'a URL requested using the previous authorization was pooled'

    issued = mocker.spy(session.raw_api, method)
    _upload(session, bucket_id, file_id, large_file=large_file)
    assert take(resource_id) == (
        issued.spy_return['uploadUrl'],
        issued.spy_return['authorizationToken'],
    )


def test_url_request_retries_expired_account_token(session_with_keys, pooled_uploads, mocker):
    session, _, bucket_id, file_id = session_with_keys
    resource_id, take, pairs = pooled_uploads
    large_file = resource_id == file_id
    for _ in pairs:
        take(resource_id)
    info = session.account_info
    expired_token = info.get_account_auth_token()
    simulator_hooks.expire_account_token(session.raw_api, expired_token)
    issued = mocker.spy(session.raw_api, 'get_upload_part_url' if large_file else 'get_upload_url')

    _upload(session, bucket_id, file_id, large_file=large_file)

    assert issued.call_count == 2
    assert issued.call_args_list[0].args[1] == expired_token
    assert issued.call_args_list[1].args[1] == info.get_account_auth_token()
    assert info.get_account_auth_token() != expired_token
    assert take(resource_id) == (
        issued.spy_return['uploadUrl'],
        issued.spy_return['authorizationToken'],
    )


@pytest.mark.parametrize('large_file', [False, True], ids=['bucket', 'part'])
def test_tagged_helpers_preserve_public_account_info_overrides(
    account_info, sqlite_account_info_factory, large_file
):
    calls = []

    class LegacyAccountInfo(type(account_info)):
        def take_bucket_upload_url(self, bucket_id):
            calls.append('take-bucket')
            return super().take_bucket_upload_url(bucket_id)

        def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
            calls.append('put-bucket')
            return super().put_bucket_upload_url(bucket_id, upload_url, upload_auth_token)

        def take_large_file_upload_url(self, file_id):
            calls.append('take-part')
            return super().take_large_file_upload_url(file_id)

        def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
            calls.append('put-part')
            return super().put_large_file_upload_url(file_id, upload_url, upload_auth_token)

    if isinstance(account_info, SqliteAccountInfo):
        info = sqlite_account_info_factory(account_info_class=LegacyAccountInfo)
    else:
        info = LegacyAccountInfo()
    generation = info._get_upload_pool_generation()
    info._put_upload_url_tagged(
        'resource', 'url', 'synthetic-token', generation, large_file=large_file
    )
    assert info._take_upload_url_tagged('resource', large_file=large_file) == (
        'url',
        'synthetic-token',
        generation,
    )
    kind = 'part' if large_file else 'bucket'
    assert calls == [f'put-{kind}', f'take-{kind}']


@pytest.mark.parametrize('reauthorize', [False, True], ids=['unchanged', 'reauthorized'])
def test_upload_preserves_public_session_getter_override(
    session_with_keys, pooled_uploads, reauthorize
):
    session, keys, bucket_id, file_id = session_with_keys
    resource_id, take, pairs = pooled_uploads
    large_file = resource_id == file_id
    for _ in pairs:
        take(resource_id)
    issued = []

    class SessionWithOverride(type(session)):
        def get_upload_url(self, bucket_id):
            if reauthorize:
                self.authorize_account(
                    'production', keys[1]['applicationKeyId'], keys[1]['applicationKey']
                )
            result = super().get_upload_url(bucket_id)
            issued.append(result)
            return result

        def get_upload_part_url(self, file_id):
            if reauthorize:
                self.authorize_account(
                    'production', keys[1]['applicationKeyId'], keys[1]['applicationKey']
                )
            result = super().get_upload_part_url(file_id)
            issued.append(result)
            return result

    overridden = SessionWithOverride(
        account_info=session.account_info,
        api_config=B2HttpApiConfig(_raw_api_class=raw_api_class(RawSimulator)),
    )
    overridden.raw_api = session.raw_api
    _upload(overridden, bucket_id, file_id, large_file=large_file)
    assert len(issued) == 1
    current_url_was_pooled = take(resource_id) == (
        issued[0]['uploadUrl'],
        issued[0]['authorizationToken'],
    )
    assert current_url_was_pooled, 'the public getter acquired a current URL that was discarded'


def test_clear_cannot_repool_url_from_removed_authorization(
    session_with_keys, pooled_uploads, mocker
):
    session, _, bucket_id, file_id = session_with_keys
    resource_id, take, pairs = pooled_uploads
    info = session.account_info
    for _ in pairs:
        take(resource_id)
    clear_paused, finish_clear, upload_progress = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    real_lock = info._auth_data_lock
    uploader = []

    class ObservedLock:
        def __enter__(self):
            if not real_lock.acquire(blocking=False):
                if uploader and threading.get_ident() == uploader[0]:
                    upload_progress.set()
                real_lock.acquire()
            return self

        def __exit__(self, *args):
            real_lock.release()

    original_reset = info._reset_upload_pools

    def pause_after_reset():
        original_reset()
        # Pause before clear has completed its account cleanup.
        clear_paused.set()
        assert finish_clear.wait(10), 'account cleanup was not released'

    def upload():
        uploader.append(threading.get_ident())
        try:
            return _upload(session, bucket_id, file_id, large_file=resource_id == file_id)
        except MissingAccountData:
            return None
        finally:
            upload_progress.set()

    mocker.patch.object(info, '_auth_data_lock', ObservedLock())
    mocker.patch.object(info, '_reset_upload_pools', side_effect=pause_after_reset)
    with ThreadPoolExecutor(max_workers=2) as executor:
        clearing = executor.submit(info.clear)
        try:
            assert clear_paused.wait(10), 'clear did not reset its pools'
            pending_upload = executor.submit(upload)
            assert upload_progress.wait(10), 'upload neither completed nor reached the lock'
        finally:
            finish_clear.set()
        clearing.result(timeout=10)
        upload_result = pending_upload.result(timeout=10)

    with pytest.raises(MissingAccountData):
        info.get_account_auth_token()
    pool_is_empty = take(resource_id) == (None, None)
    assert pool_is_empty, 'an upload refilled the pool after account data was cleared'
    assert upload_result is None, 'an upload acquired authorization during account cleanup'
