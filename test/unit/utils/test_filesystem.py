######################################################################
#
# File: test/unit/utils/test_filesystem.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import pathlib
import platform

import pytest
from apiver_deps import (
    STDOUT_FILEPATH,
    points_to_fifo,
    points_to_stdout,
    validate_b2_file_name_as_path,
)

EXPECTED_STDOUT_PATH = pathlib.Path('CON' if platform.system() == 'Windows' else '/dev/stdout')


class TestPointsToFifo:
    @pytest.mark.skipif(platform.system() == 'Windows', reason='no os.mkfifo() on Windows')
    def test_fifo_path(self, tmp_path):
        fifo_path = tmp_path / 'fifo'
        os.mkfifo(fifo_path)
        assert points_to_fifo(fifo_path) is True

    def test_non_fifo_path(self, tmp_path):
        path = tmp_path / 'subdir'
        path.mkdir(parents=True)
        assert points_to_fifo(path) is False

    def test_non_existent_path(self, tmp_path):
        path = tmp_path / 'file.txt'
        assert points_to_fifo(path) is False


class TestPointsToStdout:
    def test_stdout_path(self):
        assert points_to_stdout(EXPECTED_STDOUT_PATH) is True
        assert points_to_stdout(STDOUT_FILEPATH) is True

    def test_non_stdout_path(self, tmp_path):
        path = tmp_path / 'file.txt'
        path.touch()
        assert points_to_stdout(path) is False

    def test_non_existent_stdout_path(self, tmp_path):
        path = tmp_path / 'file.txt'
        assert points_to_stdout(path) is False


class TestValidateB2FileNameAsPath:
    @pytest.mark.parametrize('file_name', ['file.txt', 'dir/file.txt', 'dir\\file.txt'])
    def test_valid_file_name(self, file_name):
        validate_b2_file_name_as_path(file_name)

    @pytest.mark.parametrize(
        ('file_name', 'reason_pattern'),
        [
            (
                '../file.txt',
                r'relative path',
            ),
            (
                './file.txt',
                r'relative path',
            ),
            (
                'dir/../file.txt',
                r'relative path',
            ),
            (
                'dir//file.txt',
                r'relative path',
            ),
            ('/file.txt', r'absolute path'),
            ('\\file.txt', r'absolute path'),
        ],
    )
    def test_invalid_file_name(self, file_name, reason_pattern):
        with pytest.raises(ValueError, match=reason_pattern):
            validate_b2_file_name_as_path(file_name)

    @pytest.mark.skipif(
        platform.system() != 'Windows',
        reason='drive letters in paths are only forbidden on Windows',
    )
    def test_drive_letter_disallowed_on_windows(self):
        with pytest.raises(
            ValueError,
            match=r'drive letters',
        ):
            validate_b2_file_name_as_path(r'C:\file.txt')


@pytest.mark.parametrize(
    'file_name',
    ['CON', 'folder/nul.txt', r'folder\PRN.log', 'aux ', 'dir/COM1.txt', 'lpt³', 'file:stream'],
)
def test_windows_device_names_and_streams_are_rejected(monkeypatch, file_name):
    from b2sdk._internal.utils import filesystem

    monkeypatch.setattr(filesystem, '_IS_WINDOWS', True)
    with pytest.raises(ValueError):
        filesystem.validate_b2_file_name_as_path(file_name)


@pytest.mark.parametrize(
    'file_name', ['console.txt', 'com0', 'com10', 'lpt40.logfile', 'folder/file.txt']
)
def test_windows_regular_names_are_allowed(monkeypatch, file_name):
    from b2sdk._internal.utils import filesystem

    monkeypatch.setattr(filesystem, '_IS_WINDOWS', True)
    filesystem.validate_b2_file_name_as_path(file_name)


@pytest.mark.parametrize('file_name', ['CON', 'folder/nul.txt', 'file:stream'])
def test_unix_allows_windows_device_names_and_stream_characters(monkeypatch, file_name):
    from b2sdk._internal.utils import filesystem

    monkeypatch.setattr(filesystem, '_IS_WINDOWS', False)
    filesystem.validate_b2_file_name_as_path(file_name)
