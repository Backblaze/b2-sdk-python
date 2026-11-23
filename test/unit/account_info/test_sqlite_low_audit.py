######################################################################
#
# File: test/unit/account_info/test_sqlite_low_audit.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import stat
import threading

import pytest
from apiver_deps import SqliteAccountInfo


@pytest.mark.skipif(os.name == 'nt', reason='POSIX directory permissions')
def test_c58_new_config_directory_is_private(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('B2_ACCOUNT_INFO', raising=False)
    SqliteAccountInfo.get_user_account_info_path(profile='test')
    mode = stat.S_IMODE((tmp_path / 'config' / 'b2').stat().st_mode)
    assert mode == 0o700


def test_c23_profile_rejects_trailing_path(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.delenv('B2_ACCOUNT_INFO', raising=False)
    with pytest.raises(ValueError, match='Invalid profile name'):
        SqliteAccountInfo.get_user_account_info_path(profile='ok/../../x')
    assert not (tmp_path / 'config').exists()


def test_c37_worker_reopens_after_another_thread_closes(tmp_path):
    info = SqliteAccountInfo(file_name=str(tmp_path / 'account.sqlite'))
    opened, closed = threading.Event(), threading.Event()
    result = []

    def worker():
        conn = info._get_connection()
        opened.set()
        if closed.wait(10):
            try:
                new_conn = info._get_connection()
                result.append((new_conn is not conn, new_conn.execute('select 1').fetchone()))
            except Exception as exc:
                result.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert opened.wait(10)
        info.close()
    finally:
        closed.set()
        thread.join(10)
    assert not thread.is_alive()
    assert result == [(True, (1,))]
    info.close()


def test_c37_closes_all_worker_connections_and_is_idempotent(tmp_path):
    info = SqliteAccountInfo(file_name=str(tmp_path / 'account.sqlite'))
    connections = []
    ready = threading.Barrier(4)
    release = threading.Event()
    outcomes = []

    def worker():
        conn = info._get_connection()
        connections.append(conn)
        ready.wait(timeout=10)
        if release.wait(10):
            try:
                conn.execute('select 1')
            except Exception as exc:
                outcomes.append(type(exc).__name__)
            else:
                outcomes.append('open')

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for thread in threads:
        thread.start()
    try:
        ready.wait(timeout=10)
        info.close()
        info.close()
    finally:
        release.set()
        for thread in threads:
            thread.join(10)
    assert all(not thread.is_alive() for thread in threads)
    assert len({id(conn) for conn in connections}) == 3
    assert outcomes == ['ProgrammingError'] * 3
    info.close()
