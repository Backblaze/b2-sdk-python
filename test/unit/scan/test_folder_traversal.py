######################################################################
#
# File: test/unit/scan/test_folder_traversal.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import codecs
import os
import platform
import re
import sys
from unittest.mock import MagicMock, patch

import pytest

from b2sdk._internal.scan.folder import LocalFolder
from b2sdk._internal.scan.policies import ScanPoliciesManager
from b2sdk._internal.scan.report import ProgressReport
from b2sdk._internal.utils import fix_windows_path_limit


class TestFolderTraversal:
    def test_flat_folder(self, tmp_path):

        # Create a directory structure below with initial scanning point at tmp_path/dir:
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

    @pytest.mark.skipif(
        platform.system() == 'Windows',
        reason="Windows doesn't allow / or \\ in filenames",
    )
    def test_invalid_name(self, tmp_path):

        # Create a directory structure below with initial scanning point at tmp_path/dir:
        # tmp_path
        # └── dir
        #     ├── file1.txt
        #     ├── subdir
        #     │   └── file2.txt
        #     ├── file\bad.txt
        #     └── file[DEL]bad.txt

        (tmp_path / "dir" / "subdir").mkdir(parents=True)

        (tmp_path / "dir" / "file1.txt").write_text("content1")
        (tmp_path / "dir" / "subdir" / "file2.txt").write_text("content2")
        (tmp_path / "dir" / "file\\bad.txt").write_text("bad1")
        (tmp_path / "dir" / "file\x7fbad.txt").write_text("bad2")

        reporter = ProgressReport(sys.stdout, False)
        folder = LocalFolder(str(tmp_path / "dir"))
        local_paths = folder.all_files(reporter=reporter)
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert reporter.has_errors_or_warnings()
        assert isinstance(reporter.warnings, list)
        assert sorted(reporter.warnings) == [
            f"WARNING: '{tmp_path}/dir/file\\bad.txt' path contains invalid name (file names must not contain '\\'). Skipping.",
            f"WARNING: '{tmp_path}/dir/file\\x7fbad.txt' path contains invalid name (file names must not contain DEL). Skipping.",
        ]
        reporter.close()

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "dir" / "file1.txt")),
            fix_windows_path_limit(str(tmp_path / "dir" / "subdir" / "file2.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason=
        "PyPy on Windows force-decodes non-UTF-8 filenames, which makes it impossible to test this case"
    )
    def test_invalid_unicode_filename(self, tmp_path):
        # Create a directory structure below with initial scanning point at tmp_path/dir:
        # tmp_path
        # └── dir
        #     ├── file1.txt
        #     └── XXX (invalid utf-8 filename)

        (tmp_path / "dir").mkdir(parents=True)
        (tmp_path / "dir" / "file1.txt").write_text("content1")

        foreign_encoding = "euc_jp"
        # test sanity check
        assert codecs.lookup(foreign_encoding).name != codecs.lookup(
            sys.getfilesystemencoding()
        ).name

        invalid_utf8_path = os.path.join(bytes(tmp_path), b"dir", 'てすと'.encode(foreign_encoding))
        try:
            with open(invalid_utf8_path, "wb") as f:
                f.write(b"content2")
        except (OSError, UnicodeDecodeError):
            pytest.skip("Cannot create invalid UTF-8 filename on this platform")

        reporter = ProgressReport(sys.stdout, False)
        folder = LocalFolder(str(tmp_path / "dir"))
        local_paths = folder.all_files(reporter=reporter)
        absolute_paths = [path.absolute_path for path in list(local_paths)]
        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "dir" / "file1.txt")),
        ]

        assert reporter.has_errors_or_warnings()
        assert re.match(
            r"WARNING: '.+/dir/.+' path contains invalid name "
            r"\(file name must be valid Unicode, check locale\)\. Skipping\.",
            reporter.warnings[0],
        )
        assert len(reporter.warnings) == 1

        reporter.close()

    @pytest.mark.skipif(
        platform.system() == 'Windows',
        reason="Windows doesn't allow / or \\ in filenames",
    )
    def test_invalid_directory_name(self, tmp_path):

        # Create a directory structure below with initial scanning point at tmp_path/dir:
        # tmp_path
        # └── dir
        #     ├── file1.txt
        #     └── dir\bad
        #         └── file2.txt

        (tmp_path / "dir").mkdir(parents=True)
        (tmp_path / "dir" / "file1.txt").write_text("content1")
        (tmp_path / "dir" / "dir\\bad").mkdir(parents=True)
        (tmp_path / "dir" / "dir\\bad" / "file2.txt").write_text("content2")

        reporter = ProgressReport(sys.stdout, False)
        folder = LocalFolder(str(tmp_path / "dir"))
        local_paths = folder.all_files(reporter=reporter)
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert reporter.has_errors_or_warnings()
        assert reporter.warnings == [
            f"WARNING: '{tmp_path}/dir/dir\\bad' path contains invalid name (file names must not contain '\\'). Skipping."
        ]
        reporter.close()

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "dir" / "file1.txt")),
        ]

    def test_folder_with_subfolders(self, tmp_path):

        # Create a directory structure below with initial scanning point at tmp_path:
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
        reason="Symlinks not supported on PyPy/Windows",
    )
    def test_folder_with_symlink_to_file(self, tmp_path):

        # Create a directory structure below with initial scanning point at tmp_path:
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
        reason="Symlinks not supported on PyPy/Windows",
    )
    @pytest.mark.timeout(5)
    def test_folder_with_circular_symlink(self, tmp_path):

        # Create a directory structure below with initial scanning point at tmp_path:
        # tmp_path
        # ├── dir
        # │   └── file.txt
        # └── symlink_dir -> dir

        (tmp_path / "dir").mkdir()
        (tmp_path / "dir" / "file1.txt").write_text("content1")
        symlink_dir = tmp_path / "dir" / "symlink_dir"
        symlink_dir.symlink_to(tmp_path / "dir", target_is_directory=True)

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "dir" / "file1.txt")),
            fix_windows_path_limit(str(tmp_path / "dir" / "symlink_dir" / "file1.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    @pytest.mark.timeout(5)
    def test_folder_with_symlink_to_parent(self, tmp_path):

        # Create a directory structure below with the scanning point at tmp_path/parent/child/:
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
        symlink_dir.symlink_to(tmp_path / "parent", target_is_directory=True)

        folder = LocalFolder(str(tmp_path / "parent" / "child"))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "file4.txt")),
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "grandchild" / "file5.txt")),
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "grandchild" / "symlink_dir" / "child" / "file4.txt")),
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "grandchild" / "symlink_dir" / "child" / "grandchild" / "file5.txt")),
            fix_windows_path_limit(str(tmp_path / "parent" / "child" / "grandchild" / "symlink_dir" / "file3.txt")),
        ]  # yapf: disable

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    @pytest.mark.timeout(5)
    def test_root_short_loop(self, tmp_path):

        # Create a symlink to the tmp_path directory itself
        # tmp_path
        # └── tmp_path_symlink -> tmp_path

        tmp_path_symlink = tmp_path / "tmp_path_symlink"
        tmp_path_symlink.symlink_to(tmp_path, target_is_directory=True)

        folder = LocalFolder(str(tmp_path_symlink))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == []

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    @pytest.mark.timeout(5)
    def test_root_parent_loop(self, tmp_path):

        # Create a symlink that points to the parent of the initial scanning point
        # tmp_path
        # └── start
        #     ├── file.txt
        #     └── symlink -> tmp_path

        (tmp_path / "start").mkdir()
        (tmp_path / "start" / "file.txt").write_text("content")
        (tmp_path / "start" / "symlink").symlink_to(tmp_path, target_is_directory=True)

        folder = LocalFolder(str(tmp_path / "start"))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "start" / "file.txt")),
            fix_windows_path_limit(str(tmp_path / "start" / "symlink" / "start" / "file.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    def test_symlink_that_points_deeper(self, tmp_path):

        # Create a directory structure with a symlink that points to a deeper directory
        # tmp_path
        # ├── a
        # │   └── a.txt
        # └── b
        #     ├── c
        #     │   └── c.txt
        #     └── d
        #         ├── d.txt
        #         └── e
        #             └── e.txt
        # ├── f
        # │   └── f.txt
        # └── symlink -> b/d/e

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b" / "c").mkdir(parents=True)
        (tmp_path / "b" / "c" / "c.txt").write_text("c")
        (tmp_path / "b" / "d" / "e").mkdir(parents=True)
        (tmp_path / "b" / "d" / "d.txt").write_text("d")
        (tmp_path / "b" / "d" / "e" / "e.txt").write_text("e")
        (tmp_path / "f").mkdir()
        (tmp_path / "f" / "f.txt").write_text("f")
        (tmp_path / "symlink").symlink_to(tmp_path / "b" / "d" / "e", target_is_directory=True)

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "c" / "c.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "d" / "d.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "d" / "e" / "e.txt")),
            fix_windows_path_limit(str(tmp_path / "f" / "f.txt")),
            fix_windows_path_limit(str(tmp_path / "symlink" / "e.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    def test_symlink_that_points_up(self, tmp_path):

        # Create a directory structure with a symlink that points to a upper directory
        # tmp_path
        # ├── a
        # │   └── a.txt
        # └── b
        #     ├── c
        #     │   └── c.txt
        #     └── d
        #         ├── d.txt
        #         └── e
        #             ├── symlink -> ../../a
        #             └── e.txt

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b" / "c").mkdir(parents=True)
        (tmp_path / "b" / "c" / "c.txt").write_text("c")
        (tmp_path / "b" / "d" / "e").mkdir(parents=True)
        (tmp_path / "b" / "d" / "d.txt").write_text("d")
        (tmp_path / "b" / "d" / "e" / "e.txt").write_text("e")
        (tmp_path / "b" / "d" / "e" / "symlink").symlink_to(tmp_path / "a", target_is_directory=True) # yapf: disable

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "c" / "c.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "d" / "d.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "d" / "e" / "e.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "d" / "e" / "symlink" / "a.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)
    def test_elaborate_infinite_loop(self, tmp_path):

        # Create a directory structure with an elaborate infinite loop of symlinks
        # tmp_path
        # ├── a
        # │   └── a.txt
        # ├── b -> c
        # ├── c -> d
        # ├── d -> e
        # ├── e -> b
        # └── f
        #     └── f.txt

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b").symlink_to("c")
        (tmp_path / "c").symlink_to("d")
        (tmp_path / "d").symlink_to("e")
        (tmp_path / "e").symlink_to("b")
        (tmp_path / "f").mkdir()
        (tmp_path / "f" / "f.txt").write_text("f")

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "f" / "f.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    def test_valid_symlink_pattern_where_the_link_goes_down_and_up(self, tmp_path):

        # tmp_path
        # ├── a
        # │   └── a.txt
        # ├── b -> c/d
        # ├── c
        # │   └── d
        # │       └── b.txt
        # ├── d -> e
        # ├── e
        # │   └── e.txt
        # └── f
        #     └── f.txt

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b").symlink_to(tmp_path / "c" / "d", target_is_directory=True) # yapf: disable
        (tmp_path / "c").mkdir()
        (tmp_path / "c" / "d").mkdir()
        (tmp_path / "c" / "d" / "b.txt").write_text("b")
        (tmp_path / "d").symlink_to(tmp_path / "e", target_is_directory=True)
        (tmp_path / "e").mkdir()
        (tmp_path / "e" / "e.txt").write_text("e")
        (tmp_path / "f").mkdir()
        (tmp_path / "f" / "f.txt").write_text("f")

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "b.txt")),
            fix_windows_path_limit(str(tmp_path / "c" / "d" / "b.txt")),
            fix_windows_path_limit(str(tmp_path / "d" / "e.txt")),
            fix_windows_path_limit(str(tmp_path / "e" / "e.txt")),
            fix_windows_path_limit(str(tmp_path / "f" / "f.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    def test_valid_symlink_pattern_where_the_link_goes_up_and_down(self, tmp_path):

        # Create a directory structure with a valid symlink pattern where the link goes up and down
        # tmp_path
        # ├── a
        # │   └── a.txt
        # ├── b
        # │   └── c -> ../d
        # ├── d
        # │   └── e
        # │       └── f
        # │           └── f.txt
        # └── t.txt

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b").mkdir()
        (tmp_path / "b" / "c").symlink_to(tmp_path / "d", target_is_directory=True)
        (tmp_path / "d").mkdir()
        (tmp_path / "d" / "e").mkdir()
        (tmp_path / "d" / "e" / "f").mkdir()
        (tmp_path / "d" / "e" / "f" / "f.txt").write_text("f")
        (tmp_path / "t.txt").write_text("t")

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "b" / "c" / "e" / "f" / "f.txt")),
            fix_windows_path_limit(str(tmp_path / "d" / "e" / "f" / "f.txt")),
            fix_windows_path_limit(str(tmp_path / "t.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows",
    )
    @pytest.mark.timeout(5)
    def test_loop_that_goes_down_and_up(self, tmp_path):

        # Create a directory structure with a loop that goes down and up
        # tmp_path
        # ├── a
        # │   └── a.txt
        # ├── b -> c/d
        # ├── c
        # │   └── d -> ../e
        # ├── e -> b
        # └── f
        #     └── f.txt

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b").symlink_to(tmp_path / "c" / "d", target_is_directory=True)
        (tmp_path / "c").mkdir()
        (tmp_path / "c" / "d").symlink_to(tmp_path / "e", target_is_directory=True)
        (tmp_path / "e").symlink_to("b")
        (tmp_path / "f").mkdir()
        (tmp_path / "f" / "f.txt").write_text("f")

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "f" / "f.txt")),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)
    def test_loop_that_goes_up_and_down(self, tmp_path):

        # Create a directory structure with a loop that goes up and down
        # tmp_path
        # ├── a
        # │   └── a.txt
        # ├── b
        # │   └── c -> ../d
        # ├── d
        # │   └── e
        # │       └── f -> ../../b/c
        # └── g
        #     └── g.txt

        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "a.txt").write_text("a")
        (tmp_path / "b").mkdir()
        (tmp_path / "b" / "c").symlink_to(tmp_path / "d", target_is_directory=True)
        (tmp_path / "d").mkdir()
        (tmp_path / "d" / "e").mkdir()
        (tmp_path / "d" / "e" / "f").symlink_to(tmp_path / "b" / "c", target_is_directory=True)
        (tmp_path / "g").mkdir()
        (tmp_path / "g" / "g.txt").write_text("g")

        folder = LocalFolder(str(tmp_path))

        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(tmp_path / "a" / "a.txt")),
            fix_windows_path_limit(str(tmp_path / "g" / "g.txt")),
        ]

    def test_folder_all_files__dir_excluded_by_regex(self, tmp_path):
        """
        bar$ regex should exclude bar directory and all files inside it
        """
        d1_dir = tmp_path / "d1"
        d1_dir.mkdir()
        (d1_dir / "file1.txt").touch()

        bar_dir = tmp_path / "bar"
        bar_dir.mkdir()
        (bar_dir / "file2.txt").touch()

        scan_policy = ScanPoliciesManager(exclude_dir_regexes=["bar$"])

        folder = LocalFolder(tmp_path)
        local_paths = folder.all_files(reporter=None, policies_manager=scan_policy)
        absolute_paths = [path.absolute_path for path in local_paths]

        assert absolute_paths == [
            fix_windows_path_limit(str(d1_dir / "file1.txt")),
        ]

    def test_excluded_folder_no_access_check(self, tmp_path):
        """Test that a directory is not checked for access if it is excluded."""
        # Create directories and files
        excluded_dir = tmp_path / "excluded_no_access"
        excluded_dir.mkdir()
        excluded_file = excluded_dir / "should_not_access.txt"
        excluded_file.touch()

        # Setup exclusion regex that matches the directory name
        scan_policy = ScanPoliciesManager(exclude_dir_regexes=[r"excluded_no_access$"])
        reporter = ProgressReport(sys.stdout, False)

        # Patch os.access to monitor if it is called on the excluded file
        with patch('os.access', MagicMock(return_value=True)) as mocked_access:
            folder = LocalFolder(str(tmp_path))
            list(folder.all_files(reporter=reporter, policies_manager=scan_policy))

            # Verify os.access was not called for the excluded file
            mocked_access.assert_not_called()

        reporter.close()

    def test_excluded_folder_without_permissions(self, tmp_path):
        """Test that a excluded directory without permissions is not processed and no warning is issued."""
        excluded_dir = tmp_path / "excluded_dir"
        excluded_dir.mkdir()
        (excluded_dir / "file.txt").touch()

        # Modify directory permissions to simulate lack of access
        excluded_dir.chmod(0o000)

        scan_policy = ScanPoliciesManager(exclude_dir_regexes=[r"excluded_dir$"])
        reporter = ProgressReport(sys.stdout, False)

        folder = LocalFolder(str(tmp_path))
        local_paths = folder.all_files(reporter=reporter, policies_manager=scan_policy)
        absolute_paths = [path.absolute_path for path in local_paths]

        # Restore directory permissions to clean up
        excluded_dir.chmod(0o755)

        # Check that no files from the excluded directory are processed
        assert not any(
            "excluded_dir" in path for path in absolute_paths
        ), "Files from the excluded directory were processed"

        # Check that no access warnings are issued for the excluded directory
        assert not reporter.warnings == [
            f"WARNING: {tmp_path}/excluded_dir could not be accessed (no permissions to read?)"
        ], "Access warning was issued for the excluded directory"

        reporter.close()
