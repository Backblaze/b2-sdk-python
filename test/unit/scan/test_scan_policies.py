######################################################################
#
# File: test/unit/scan/test_scan_policies.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
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
        d = tmp_path / "dir"
        d.mkdir()
        (d / "file1.txt").write_text("content1")
        (d / "file2.txt").write_text("content2")
        (d / "file3.txt").write_text("content3")

        folder = LocalFolder(str(d))
        local_paths = folder.all_files(reporter=MagicMock())
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(d / "file1.txt")),
            fix_windows_path_limit(str(d / "file2.txt")),
            fix_windows_path_limit(str(d / "file3.txt")),
        ]

    def test_folder_with_subfolders(self, tmp_path):
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
        d = tmp_path / "dir"
        d.mkdir()

        file = d / "file.txt"
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

    #FIXME the following two tests could be combined to avoid code duplication
    # but I decide to keep them separate for now to make it easier to debug

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)  # Set a 5-second timeout for this test
    def test_folder_with_circular_symlink(self, tmp_path):
        d = tmp_path / "dir"
        d.mkdir()

        (d / "file1.txt").write_text("content1")

        symlink_dir = d / "symlink_dir"
        symlink_dir.symlink_to(d)

        folder = LocalFolder(str(tmp_path))

        policies_manager = ScanPoliciesManager()
        policies_manager.max_symlink_visits = 3

        local_paths = folder.all_files(reporter=MagicMock(), policies_manager=policies_manager)
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(d / "file1.txt")),
            fix_windows_path_limit(str(d / "symlink_dir" / "file1.txt")),
            fix_windows_path_limit(str(d / "symlink_dir" / "symlink_dir" / "file1.txt")),
            fix_windows_path_limit(
                str(d / "symlink_dir" / "symlink_dir" / "symlink_dir" / "file1.txt")
            ),
        ]

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)  # Set a 5-second timeout for this test
    def test_circular_symlink_with_limit_1(self, tmp_path):
        d = tmp_path / "dir"
        d.mkdir()

        (d / "file1.txt").write_text("content1")

        symlink_dir = d / "symlink_dir"
        symlink_dir.symlink_to(d)

        policies_manager = ScanPoliciesManager()
        policies_manager.max_symlink_visits = 1

        folder = LocalFolder(str(tmp_path))
        local_paths = folder.all_files(reporter=MagicMock(), policies_manager=policies_manager)
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(str(d / "file1.txt")),
            fix_windows_path_limit(str(d / "symlink_dir" / "file1.txt")),
        ]

    # Just for fun, another test, based on a comment by @zackse available here:
    # https://github.com/Backblaze/B2_Command_Line_Tool/issues/513#issuecomment-426751216

    @pytest.mark.skipif(
        platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
        reason="Symlinks not supported on PyPy/Windows"
    )
    @pytest.mark.timeout(5)  # Set a 5-second timeout for this test
    def test_circular_interesting_case(self, tmp_path):

        # Create directories
        o = tmp_path / "outsidedir/four/five/six"
        o.mkdir(parents=True)

        s = tmp_path / "startdir/one/two/three"
        s.mkdir(parents=True)

        # Create symbolic links
        symlink_dir = tmp_path / "outsidedir/four/five/one"
        symlink_dir.symlink_to(tmp_path / "startdir/one")

        symlink_dir = tmp_path / "startdir/badlink"
        symlink_dir.symlink_to("/")

        symlink_dir = tmp_path / "startdir/outsidedir"
        symlink_dir.symlink_to(tmp_path / "outsidedir")

        # Create text files
        file = tmp_path / "outsidedir/hello.txt"
        file.write_text("hello")

        file = tmp_path / "startdir/hello.txt"
        file.write_text("hello")

        file = tmp_path / "startdir/one/goodbye.txt"
        file.write_text("goodbye")

        folder = LocalFolder(str(tmp_path))

        policies_manager = ScanPoliciesManager()
        policies_manager.max_symlink_visits = 3

        local_paths = folder.all_files(reporter=MagicMock(), policies_manager=policies_manager)
        absolute_paths = [path.absolute_path for path in list(local_paths)]

        assert absolute_paths == [
            fix_windows_path_limit(
                str(tmp_path / "outsidedir" / "four" / "five" / "one" / "goodbye.txt")
            ),
            fix_windows_path_limit(str(tmp_path / "outsidedir" / "hello.txt")),
            fix_windows_path_limit(str(tmp_path / "startdir" / "hello.txt")),
            fix_windows_path_limit(str(tmp_path / "startdir" / "one" / "goodbye.txt")),
            fix_windows_path_limit(
                str(tmp_path / "startdir" / "outsidedir" / "four" / "five" / "one" / "goodbye.txt")
            ),
            fix_windows_path_limit(str(tmp_path / "startdir" / "outsidedir" / "hello.txt")),
        ]


class TestScanPoliciesManager:
    def test_include_file_regexes_without_exclude(self):
        kwargs = {'include_file_regexes': '.*'}  # valid regex
        with pytest.raises(InvalidArgument):
            ScanPoliciesManager(**kwargs)

    @pytest.mark.parametrize(
        'param,exception',
        [
            pytest.param(
                'exclude_dir_regexes', InvalidArgument, marks=pytest.mark.apiver(from_ver=2)
            ),
            pytest.param(
                'exclude_file_regexes', InvalidArgument, marks=pytest.mark.apiver(from_ver=2)
            ),
            pytest.param(
                'include_file_regexes', InvalidArgument, marks=pytest.mark.apiver(from_ver=2)
            ),
            pytest.param('exclude_dir_regexes', re.error, marks=pytest.mark.apiver(to_ver=1)),
            pytest.param('exclude_file_regexes', re.error, marks=pytest.mark.apiver(to_ver=1)),
            pytest.param('include_file_regexes', re.error, marks=pytest.mark.apiver(to_ver=1)),
        ],
    )
    def test_illegal_regex(self, param, exception):
        kwargs = {
            'exclude_dir_regexes': '.*',
            'exclude_file_regexes': '.*',
            'include_file_regexes': '.*',
            param: '*',  # invalid regex
        }
        with pytest.raises(exception):
            ScanPoliciesManager(**kwargs)

    @pytest.mark.parametrize(
        'param,exception',
        [
            pytest.param(
                'exclude_modified_before', InvalidArgument, marks=pytest.mark.apiver(from_ver=2)
            ),
            pytest.param(
                'exclude_modified_after', InvalidArgument, marks=pytest.mark.apiver(from_ver=2)
            ),
            pytest.param('exclude_modified_before', ValueError, marks=pytest.mark.apiver(to_ver=1)),
            pytest.param('exclude_modified_after', ValueError, marks=pytest.mark.apiver(to_ver=1)),
        ],
    )
    def test_illegal_timestamp(self, param, exception):
        kwargs = {
            'exclude_modified_before': 1,
            'exclude_modified_after': 2,
            param: -1.0,  # invalid range param
        }
        with pytest.raises(exception):
            ScanPoliciesManager(**kwargs)
