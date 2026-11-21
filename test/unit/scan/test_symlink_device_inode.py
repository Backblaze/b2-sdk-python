######################################################################
#
# File: test/unit/scan/test_symlink_device_inode.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
from pathlib import Path

from b2sdk._internal.scan.folder import LocalFolder


def test_symlink_targets_with_same_inode_on_different_devices_are_scanned(tmp_path, monkeypatch):
    first = tmp_path / 'first'
    second = first / 'second'
    first.mkdir()
    second.mkdir()
    (second / 'file.txt').write_text('contents')
    (first / 'next').symlink_to(second, target_is_directory=True)
    root = tmp_path / 'root'
    root.symlink_to(first, target_is_directory=True)

    original_stat = Path.stat

    def stat_with_device_collision(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path in (first, second):
            fields = list(result)
            fields[1] = 12345
            fields[2] = 1 if path == first else 2
            return os.stat_result(fields)
        return result

    monkeypatch.setattr(Path, 'stat', stat_with_device_collision)
    files = list(LocalFolder(root).all_files(None))

    assert [path.relative_path for path in files] == ['next/file.txt', 'second/file.txt']
