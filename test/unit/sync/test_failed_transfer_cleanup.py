######################################################################
#
# File: test/unit/sync/test_failed_transfer_cleanup.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import time

import pytest

from b2sdk._internal.scan.folder import B2Folder, LocalFolder
from b2sdk._internal.sync.action import B2DeleteAction
from b2sdk._internal.sync.exception import IncompleteSync
from b2sdk._internal.sync.policy import CompareVersionMode
from b2sdk._internal.sync.sync import KeepOrDeleteMode, Synchronizer
from test.unit import simulator_hooks

pytestmark = pytest.mark.apiver(3)


@pytest.mark.parametrize('workers', [1, 4])
@pytest.mark.parametrize('mode', [KeepOrDeleteMode.DELETE, KeepOrDeleteMode.KEEP_BEFORE_DELETE])
def test_failed_replacement_preserves_old_versions(
    tmp_path, b2api, bucket, monkeypatch, workers, mode
):
    old = [bucket.upload_bytes(data, 'file.txt') for data in (b'old', b'older')]
    old_versions = [v.id_ for v in old]
    (tmp_path / 'file.txt').write_bytes(b'replacement bytes')
    dest = B2Folder(bucket.name, '', b2api)

    def fail_upload(*args, **kwargs):
        raise RuntimeError('replacement failed')

    monkeypatch.setattr(dest.bucket, 'concatenate', fail_upload)
    sync = Synchronizer(
        max_workers=workers,
        keep_days_or_delete=mode,
        keep_days=1 if mode == KeepOrDeleteMode.KEEP_BEFORE_DELETE else None,
        compare_version_mode=CompareVersionMode.SIZE,
    )
    with pytest.raises(IncompleteSync):
        sync.sync_folders(
            LocalFolder(tmp_path), dest, max(v.upload_timestamp for v in old) + 3 * 86400000, None
        )
    assert {v.id_ for v in bucket.list_file_versions('file.txt')} == set(old_versions)


def test_successful_replacement_deletes_old_versions(tmp_path, b2api, bucket):
    old_ids = {bucket.upload_bytes(data, 'file.txt').id_ for data in (b'old', b'older')}
    (tmp_path / 'file.txt').write_bytes(b'replacement bytes')
    sync = Synchronizer(
        max_workers=4,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )
    sync.sync_folders(
        LocalFolder(tmp_path), B2Folder(bucket.name, '', b2api), int(time.time() * 1000), None
    )
    versions = list(bucket.list_file_versions('file.txt'))
    assert len(versions) == 1
    assert versions[0].id_ not in old_ids
    assert versions[0].size == len(b'replacement bytes')


def test_cleanup_waits_for_in_progress_upload(tmp_path, b2api, bucket, monkeypatch):
    from threading import Event, Thread

    old_id = bucket.upload_bytes(b'old', 'file.txt').id_
    (tmp_path / 'file.txt').write_bytes(b'replacement bytes')
    dest = B2Folder(bucket.name, '', b2api)
    started, release, scheduled, delete_started = Event(), Event(), Event(), Event()
    errors = []
    real_upload = dest.bucket.concatenate

    def wait_for_upload(*args, **kwargs):
        started.set()
        assert release.wait(10), 'upload was not released'
        return real_upload(*args, **kwargs)

    monkeypatch.setattr(dest.bucket, 'concatenate', wait_for_upload)
    sync = Synchronizer(
        max_workers=4,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )

    original_actions = sync._make_folder_sync_actions
    original_delete = B2DeleteAction.run

    def tracked_actions(*args, **kwargs):
        yield from original_actions(*args, **kwargs)
        scheduled.set()

    def tracked_delete(self, *args, **kwargs):
        delete_started.set()
        return original_delete(self, *args, **kwargs)

    monkeypatch.setattr(sync, '_make_folder_sync_actions', tracked_actions)
    monkeypatch.setattr(B2DeleteAction, 'run', tracked_delete)

    def run_sync():
        try:
            sync.sync_folders(LocalFolder(tmp_path), dest, int(time.time() * 1000), None)
        except Exception as exc:
            errors.append(exc)

    thread = Thread(target=run_sync)
    thread.start()
    try:
        assert started.wait(10), 'upload did not start'
        assert scheduled.wait(10), 'scheduler did not finish'
        assert not delete_started.wait(0.3), 'delete ran while upload was blocked'
        assert [v.id_ for v in bucket.list_file_versions('file.txt')] == [old_id]
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive()
    assert not errors
    assert old_id not in {v.id_ for v in bucket.list_file_versions('file.txt')}


def test_failed_upload_does_not_block_another_file(tmp_path, b2api, bucket, monkeypatch):
    failed_id = bucket.upload_bytes(b'old', 'a.txt').id_
    other_id = bucket.upload_bytes(b'old', 'b.txt').id_
    (tmp_path / 'a.txt').write_bytes(b'new contents')
    (tmp_path / 'b.txt').write_bytes(b'new contents')
    dest = B2Folder(bucket.name, '', b2api)
    original = dest.bucket.concatenate

    def fail_one(sources, name, *args, **kwargs):
        if name == 'a.txt':
            raise RuntimeError('replacement failed')
        return original(sources, name, *args, **kwargs)

    monkeypatch.setattr(dest.bucket, 'concatenate', fail_one)
    sync = Synchronizer(
        max_workers=2,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )
    with pytest.raises(IncompleteSync):
        sync.sync_folders(LocalFolder(tmp_path), dest, int(time.time() * 1000), None)
    assert {v.id_ for v in bucket.list_file_versions('a.txt')} == {failed_id}
    other_versions = list(bucket.list_file_versions('b.txt'))
    assert len(other_versions) == 1
    assert other_versions[0].id_ != other_id
    assert other_versions[0].size == len(b'new contents')


def test_failed_copy_preserves_old_versions(b2api, bucket, monkeypatch):
    source_bucket = b2api.create_bucket('source-bucket', 'allPublic')
    source_bucket.upload_bytes(b'replacement contents', 'file.txt')
    old_ids = {bucket.upload_bytes(data, 'file.txt').id_ for data in (b'old', b'older')}
    source = B2Folder(source_bucket.name, '', b2api)
    dest = B2Folder(bucket.name, '', b2api)

    def fail_copy(*args, **kwargs):
        raise RuntimeError('copy failed')

    monkeypatch.setattr(dest.bucket, 'copy', fail_copy)
    sync = Synchronizer(
        max_workers=2,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )
    with pytest.raises(IncompleteSync):
        sync.sync_folders(source, dest, int(time.time() * 1000), None)
    assert {v.id_ for v in bucket.list_file_versions('file.txt')} == old_ids


def test_blocked_transfer_does_not_starve_unrelated_file(tmp_path, b2api, bucket, monkeypatch):
    from threading import Event, Thread

    for content in (b'old', b'older', b'oldest'):
        bucket.upload_bytes(content, 'a.txt')
    (tmp_path / 'a.txt').write_bytes(b'replacement contents')
    (tmp_path / 'b.txt').write_bytes(b'unrelated contents')
    dest = B2Folder(bucket.name, '', b2api)
    started, release, other_done = Event(), Event(), Event()
    errors = []
    original = dest.bucket.concatenate

    def upload(sources, name, *args, **kwargs):
        if name == 'a.txt':
            started.set()
            assert release.wait(10), 'blocked upload was not released'
        result = original(sources, name, *args, **kwargs)
        if name == 'b.txt':
            other_done.set()
        return result

    monkeypatch.setattr(dest.bucket, 'concatenate', upload)
    sync = Synchronizer(
        max_workers=2,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )

    def run_sync():
        try:
            sync.sync_folders(LocalFolder(tmp_path), dest, int(time.time() * 1000), None)
        except Exception as exc:
            errors.append(exc)

    thread = Thread(target=run_sync)
    thread.start()
    try:
        assert started.wait(10), 'first upload did not start'
        assert other_done.wait(2), 'unrelated upload was starved by cleanup tasks'
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive()
    assert not errors


def test_ready_cleanup_is_bounded_during_enumeration(tmp_path, b2api, bucket, monkeypatch):
    import queue
    from types import SimpleNamespace

    import b2sdk._internal.sync.sync as sync_module

    simulator_hooks.allow_unlimited_upload_urls(b2api.session.raw_api, bucket.id_)
    max_ready = []

    class TrackedQueue(queue.SimpleQueue):
        def put(self, item):
            super().put(item)
            max_ready.append(self.qsize())

    monkeypatch.setattr(
        sync_module, 'queue', SimpleNamespace(SimpleQueue=TrackedQueue, Empty=queue.Empty)
    )
    old_ids = set()
    for index in range(1010):
        name = f'{index:04}.txt'
        old_ids.add(bucket.upload_bytes(b'old', name).id_)
        (tmp_path / name).write_bytes(b'new content')

    sync = Synchronizer(
        max_workers=1,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )
    original_actions = sync._make_folder_sync_actions

    def slow_actions(*args, **kwargs):
        for action in original_actions(*args, **kwargs):
            is_transfer = vars(action).get('_sync_transfer') is None
            yield action
            if is_transfer:
                action._sync_future.result(timeout=10)

    monkeypatch.setattr(sync, '_make_folder_sync_actions', slow_actions)
    sync.sync_folders(
        LocalFolder(tmp_path), B2Folder(bucket.name, '', b2api), int(time.time() * 1000), None
    )
    assert max_ready
    assert max(max_ready) <= 1
    remaining_ids = {version.id_ for version, _ in bucket.ls('', latest_only=False, recursive=True)}
    assert len(old_ids) == 1010
    assert not old_ids.intersection(remaining_ids)


@pytest.mark.parametrize('fail', [False, True])
def test_pending_cleanup_backpressure_with_one_worker(tmp_path, b2api, bucket, monkeypatch, fail):
    from threading import Event, Thread

    simulator_hooks.allow_unlimited_upload_urls(b2api.session.raw_api, bucket.id_)
    old_ids = {bucket.upload_bytes(b'old', 'file.txt').id_ for _ in range(1002)}
    (tmp_path / 'file.txt').write_bytes(b'replacement contents')
    dest = B2Folder(bucket.name, '', b2api)
    started, release, at_limit, beyond_limit = Event(), Event(), Event(), Event()
    original_upload = dest.bucket.concatenate
    errors = []

    def blocked_upload(*args, **kwargs):
        started.set()
        assert release.wait(10), 'upload was not released'
        if fail:
            raise RuntimeError('replacement failed')
        return original_upload(*args, **kwargs)

    monkeypatch.setattr(dest.bucket, 'concatenate', blocked_upload)
    sync = Synchronizer(
        max_workers=1,
        keep_days_or_delete=KeepOrDeleteMode.DELETE,
        compare_version_mode=CompareVersionMode.SIZE,
    )
    original_actions = sync._make_folder_sync_actions

    def tracked_actions(*args, **kwargs):
        count = 0
        for action in original_actions(*args, **kwargs):
            if vars(action).get('_sync_transfer') is not None:
                count += 1
                if count == 1001:
                    at_limit.set()
                if count == 1002:
                    beyond_limit.set()
            yield action

    monkeypatch.setattr(sync, '_make_folder_sync_actions', tracked_actions)

    def run_sync():
        try:
            sync.sync_folders(LocalFolder(tmp_path), dest, int(time.time() * 1000), None)
        except Exception as exc:
            errors.append(exc)

    thread = Thread(target=run_sync, daemon=True)
    thread.start()
    try:
        assert started.wait(10)
        assert at_limit.wait(10), 'scheduler did not reach the bound'
        assert not beyond_limit.wait(0.3), 'scheduler exceeded the dependency bound'
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive(), 'sync deadlocked after transfer completion'
    remaining = {v.id_ for v in bucket.list_file_versions('file.txt')}
    if fail:
        assert len(errors) == 1 and isinstance(errors[0], IncompleteSync)
        assert remaining == old_ids
    else:
        assert not errors
        assert len(remaining) == 1 and not remaining.intersection(old_ids)
