######################################################################
#
# File: test/unit/sync/test_scan_policies.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import ScanPoliciesManager
from apiver_deps_exception import InvalidArgument


class TestScanPoliciesManager:
    @pytest.mark.parametrize(
        'param',
        [
            'exclude_dir_regexes',
            'exclude_file_regexes',
            'include_file_regexes',
        ],
    )
    def test_illegal_regex(self, param):
        kwargs = {param: '*'}
        with pytest.raises(InvalidArgument):
            ScanPoliciesManager(**kwargs)

    @pytest.mark.parametrize(
        'param',
        [
            'exclude_modified_before',
            'exclude_modified_after',
        ],
    )
    def test_illegal_timestamp(self, param):
        kwargs = {param: -1.0}
        with pytest.raises(InvalidArgument):
            ScanPoliciesManager(**kwargs)
