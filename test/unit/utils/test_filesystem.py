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
)

EXPECTED_STDOUT_PATH = pathlib.Path("CON" if platform.system() == "Windows" else "/dev/stdout")


class TestPointsToFifo:
    @pytest.mark.skipif(platform.system() == "Windows", reason="no os.mkfifo() on Windows")
    def test_fifo_path(self, tmp_path):
        fifo_path = tmp_path / "fifo"
        os.mkfifo(fifo_path)
        assert points_to_fifo(fifo_path) is True

    def test_non_fifo_path(self, tmp_path):
        path = tmp_path / "subdir"
        path.mkdir(parents=True)
        assert points_to_fifo(path) is False

    def test_non_existent_path(self, tmp_path):
        path = tmp_path / "file.txt"
        assert points_to_fifo(path) is False


class TestPointsToStdout:
    def test_stdout_path(self):
        assert points_to_stdout(EXPECTED_STDOUT_PATH) is True
        assert points_to_stdout(STDOUT_FILEPATH) is True

    def test_non_stdout_path(self, tmp_path):
        path = tmp_path / "file.txt"
        path.touch()
        assert points_to_stdout(path) is False

    def test_non_existent_stdout_path(self, tmp_path):
        path = tmp_path / "file.txt"
        assert points_to_stdout(path) is False
