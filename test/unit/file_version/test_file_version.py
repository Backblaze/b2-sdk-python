######################################################################
#
# File: test/unit/file_version/test_file_version.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import FileVersionInfo


class TestFileVersion:
    @pytest.mark.apiver(to_ver=1)
    def test_format_ls_entry(self):
        file_version_info = FileVersionInfo(
            'a2', 'inner/a.txt', 200, 'text/plain', 'sha1', {}, 2000, 'upload'
        )
        expected_entry = (
            '                                                       '
            '                          a2  upload  1970-01-01  '
            '00:00:02        200  inner/a.txt'
        )
        assert expected_entry == file_version_info.format_ls_entry()
