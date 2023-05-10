######################################################################
#
# File: test/unit/scan/test_folder_traversal.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from pathlib import Path
import platform
import re
from unittest.mock import MagicMock
from b2sdk.scan.folder import LocalFolder
from b2sdk.utils import fix_windows_path_limit
import pytest

from apiver_deps import ScanPoliciesManager
from apiver_deps_exception import InvalidArgument


class TestFolderTraversal:
    def test_flat_folder(self, tmp_path):

        # Create a directory structure below with initial scannig point at tmp_path/dir:
        # tmp_path
        # └── dir
        #     ├── file1.txt
        #     ├── file2.txt
        #     └── file3.txt

        (tmp_path / "dir").mkdir(parents=True)

        (tmp_path / "dir" / "file1.txt").write_text("content1")
        (tmp_path / "dir" / "file2.txt").write_text("content2")
        (tmp_path / "dir" / "file3.txt").write_text("content3")

        folder = LocalFolder(str(tmp_path / "dir"))
        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "dir" / "file1.txt")),
            fix_windows_path_limit(str(tmp_path / "dir" / "file2.txt")),
            fix_windows_path_limit(str(tmp_path / "dir" / "file3.txt")),
        ]

    def test_folder_with_subfolders(self, tmp_path):

        # Create a directory structure below with initial scannig point at tmp_path:
        # tmp_path
        # ├── dir1
        # │   ├── file1.txt
        # │   └── file2.txt
        # └── dir2
        #     ├── file3.txt
        #     └── file4.txt

        d1 = tmp_path / "dir1"
        d1.mkdir()
        (d1 / "file1.txt").write_text("content1")
        (d1 / "file2.txt").write_text("content2")

        d2 = tmp_path / "dir2"
        d2.mkdir()
        (d2 / "file3.txt").write_text("content3")
        (d2 / "file4.txt").write_text("content4")

        folder = LocalFolder(str(tmp_path))
        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(d1 / "file1.txt")),
            fix_windows_path_limit(str(d1 / "file2.txt")),
            fix_windows_path_limit(str(d2 / "file3.txt")),
            fix_windows_path_limit(str(d2 / "file4.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    def test_folder_with_symlink_to_file(self, tmp_path):

        # Create a directory structure below with initial scannig point at tmp_path:
        # tmp_path
        # ├── dir
        # │   └── file.txt
        # └── symlink_file.txt -> dir/file.txt

        (tmp_path / "dir").mkdir()

        file = tmp_path / "dir" / "file.txt"
        file.write_text("content")

        symlink_file = tmp_path / "symlink_file.txt"
        symlink_file.symlink_to(file)

        folder = LocalFolder(str(tmp_path))
        local_paths = folder.all_files(reporter=MagicMock())

        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(file)),
            fix_windows_path_limit(str(symlink_file))
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)  # Set a 5-second timeout for this test
    def test_folder_with_circular_symlink(self, tmp_path):

        # Create a directory structure below with initial scannig point at tmp_path:
        # tmp_path
        # ├── dir
        # │   └── file.txt
        # └── symlink_dir -> dir

        (tmp_path / "dir").mkdir()

        (tmp_path / "dir" / "file1.txt").write_text("content1")

        symlink_dir = tmp_path / "dir" / "symlink_dir"
        symlink_dir.symlink_to(tmp_path / "dir")

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "dir" / "file1.txt")),
            fix_windows_path_limit(str(tmp_path / "dir" / "symlink_dir" / "file1.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)  # Set a 5-second timeout for this test
    def test_folder_with_symlink_to_parent(self, tmp_path):

        # Create a directory structure below with the scannig point at tmp_path/parent/child/:
        #   tmp_path
        #   ├── parent
        #   │   ├── child
        #   │   │   ├── file4.txt
        #   │   │   └── grandchild
        #   │   │       ├── file5.txt
        #   │   │       └── symlink_dir -> ../../.. (symlink to tmp_path/parent)
        #   │   └── file3.txt
        #   ├── file1.txt
        #   └── file2.txt

        (tmp_path / "parent" / "child" / "grandchild").mkdir(parents=True)

        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")

        (tmp_path / "parent" / "file3.txt").write_text("content3")

        (tmp_path / "parent" / "child" / "file4.txt").write_text("content4")

        (tmp_path / "parent" / "child" / "grandchild" / "file5.txt").write_text("content5")
        symlink_dir = tmp_path / "parent" / "child" / "grandchild" / "symlink_dir"
        symlink_dir.symlink_to(tmp_path / "parent")

        folder = LocalFolder(str(tmp_path / "parent" / "child"))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "file4.txt")),
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "grandchild" / "file5.txt")),
            fix_windows_path_limit(
                str(
                    tmp_path / "parent" / "child" / "grandchild" / "symlink_dir" / "child" /
                    "file4.txt"
                )
            ),
            fix_windows_path_limit(
                str(
                    tmp_path / "parent" / "child" / "grandchild" / "symlink_dir" / "child" /
                    "grandchild" / "file5.txt"
                )
            ),
            fix_windows_path_limit(
                str(tmp_path / "parent" / "child" / "grandchild" / "symlink_dir" / "file3.txt")
            ),
        ]  # yapf: disable
