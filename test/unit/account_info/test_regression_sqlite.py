######################################################################
#
# File: test/unit/account_info/test_regression_sqlite.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for SqliteAccountInfo.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import os
import threading

import pytest
from apiver_deps import (
    B2_ACCOUNT_INFO_ENV_VAR,
    XDG_CONFIG_HOME_ENV_VAR,
    SqliteAccountInfo,
)

from test.known_defect import known_defect

pytestmark = [pytest.mark.apiver(from_ver=2)]


@pytest.fixture
def home(monkeypatch, tmp_path):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('USERPROFILE', str(tmp_path))
    monkeypatch.delenv(B2_ACCOUNT_INFO_ENV_VAR, raising=False)
    monkeypatch.delenv(XDG_CONFIG_HOME_ENV_VAR, raising=False)
    return tmp_path


class TestProfilePaths:
    @known_defect(
        'C47',
        'an existing ~/.b2db-<profile>.sqlite is ignored because "~" is not expanded before the existence check',
    )
    def test_c47_existing_legacy_profile_database_is_used_with_xdg(self, home, monkeypatch):
        """C47: an existing ~/.b2db-<profile>.sqlite must be found even when XDG_CONFIG_HOME is set."""
        monkeypatch.setenv(XDG_CONFIG_HOME_ENV_VAR, str(home / 'xdg'))
        legacy = home / '.b2db-foo.sqlite'
        legacy.write_bytes(b'')
        path = SqliteAccountInfo.get_user_account_info_path(profile='foo')
        assert os.path.realpath(path) == os.path.realpath(str(legacy)), f'got {path}'


class TestThreadConnections:
    @known_defect('C37', "close() closes only the calling thread's database connection")
    def test_c37_close_closes_connections_of_other_threads(self, tmp_path):
        """C37: close() must close the database connections opened by other threads, too."""
        info = SqliteAccountInfo(file_name=str(tmp_path / 'acct.sqlite'))
        opened, closed_by_main, result = threading.Event(), threading.Event(), {}

        def worker():
            conn = info._get_connection()  # connection of this worker thread
            opened.set()
            assert closed_by_main.wait(timeout=10), 'main thread did not call close()'
            try:
                conn.execute('select 1')
                result['closed'] = False
            except Exception:  # sqlite3.ProgrammingError: Cannot operate on a closed database
                result['closed'] = True

        thread = threading.Thread(target=worker)
        thread.start()
        assert opened.wait(timeout=10), 'worker thread did not open a connection'
        info.close()
        closed_by_main.set()
        thread.join(timeout=10)
        assert (
            result.get('closed') is True
        ), 'connection of another thread is still open after close()'
