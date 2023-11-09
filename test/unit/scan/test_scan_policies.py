######################################################################
#
# File: test/unit/scan/test_scan_policies.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import re

import pytest
from apiver_deps import ScanPoliciesManager
from apiver_deps_exception import InvalidArgument


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

    @pytest.mark.apiver(from_ver=2)
    def test_re_pattern_argument_support(self):
        kwargs = {
            param: (re.compile(r".*"),)
            for param in (
                "exclude_dir_regexes",
                "exclude_file_regexes",
                "include_file_regexes",
            )
        }
        ScanPoliciesManager(**kwargs)
