######################################################################
#
# File: test/integration/test_sync.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io
import time

import pytest

from b2sdk.v3 import (
    CompareVersionMode,
    NewerFileSyncMode,
    Synchronizer,
    SyncReport,
    parse_folder,
)


@pytest.fixture
def local_folder_with_files(tmp_path):
    folder = tmp_path / 'test'
    folder.mkdir()
    (folder / 'a').mkdir()
    (folder / 'a' / 'foo').write_bytes(b'foo')
    # space in the name is important as it influences lexicographical sorting used by B2
    (folder / 'a b').mkdir()
    (folder / 'a b' / 'bar').write_bytes(b'bar')
    return folder


def test_sync_folder(b2_api, local_folder_with_files, b2_subfolder):
    source_folder = parse_folder(str(local_folder_with_files), b2_api)
    dest_folder = parse_folder(b2_subfolder, b2_api)

    synchronizer = Synchronizer(
        max_workers=10,
        newer_file_mode=NewerFileSyncMode.REPLACE,
        compare_version_mode=CompareVersionMode.MODTIME,
        compare_threshold=10,  # ms
    )

    def sync_and_report():
        buf = io.StringIO()
        reporter = SyncReport(buf, no_progress=True)
        with reporter:
            synchronizer.sync_folders(
                source_folder=source_folder,
                dest_folder=dest_folder,
                now_millis=int(1000 * time.time()),
                reporter=reporter,
            )
        return reporter

    report = sync_and_report()
    assert report.total_transfer_files == 2
    assert report.total_transfer_bytes == 6

    second_sync_report = sync_and_report()
    assert second_sync_report.total_transfer_files == 0
    assert second_sync_report.total_transfer_bytes == 0
