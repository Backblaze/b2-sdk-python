######################################################################
#
# File: test/unit/sync/test_sync_escaping.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io
import time

import pytest
from apiver_deps import B2Folder, LocalFolder, Synchronizer, SyncReport
from apiver_deps_exception import IncompleteSync

# U+009B (8-bit CSI) is legal in NTFS names, so the error test can create it on Windows too
HOSTILE_NAME = '\x9b31minvoice.pdf'
ESCAPED_HOSTILE_NAME = "'\\x9b31minvoice.pdf'"


@pytest.fixture
def source(b2api):
    b2api.create_bucket('source', 'allPrivate').upload_bytes(b'x', HOSTILE_NAME)
    return B2Folder('source', '', b2api)


def _sync(source, dest, out, dry_run=False):
    Synchronizer(max_workers=1, dry_run=dry_run).sync_folders(
        source_folder=source,
        dest_folder=dest,
        now_millis=int(time.time() * 1000),
        reporter=SyncReport(out, True),
    )


def test_download_completion_escapes_remote_name(source, tmp_path):
    out = io.StringIO()
    _sync(source, LocalFolder(str(tmp_path)), out, dry_run=True)

    assert out.getvalue().rstrip() == f'dnload {ESCAPED_HOSTILE_NAME}'


def test_copy_completion_escapes_remote_name(b2api, source):
    b2api.create_bucket('dest', 'allPrivate')
    out = io.StringIO()
    _sync(source, B2Folder('dest', '', b2api), out, dry_run=True)

    assert out.getvalue().rstrip() == f'copy {ESCAPED_HOSTILE_NAME}'


@pytest.mark.apiver(from_ver=1)
def test_failed_action_error_escapes_remote_name(source, tmp_path):
    (tmp_path / HOSTILE_NAME).mkdir()
    out = io.StringIO()
    with pytest.raises(IncompleteSync):
        _sync(source, LocalFolder(str(tmp_path)), out)

    assert 'b2_download(\\x9b31minvoice.pdf' in out.getvalue()
    assert HOSTILE_NAME not in out.getvalue()
