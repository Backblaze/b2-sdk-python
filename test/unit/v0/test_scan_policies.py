######################################################################
#
# File: test/unit/v0/test_scan_policies.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from ..test_base import TestBase
from .deps import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from .deps_exception import InvalidArgument


class TestScanPolicies(TestBase):
    def test_default(self):
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_directory(''))
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_file(''))
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_directory('a'))
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_file('a'))

    def test_exclude_include(self):
        policy = ScanPoliciesManager(exclude_file_regexes=('a', 'b'), include_file_regexes=('ab',))
        self.assertTrue(policy.should_exclude_file('alfa'))
        self.assertTrue(policy.should_exclude_file('bravo'))
        self.assertFalse(policy.should_exclude_file('abend'))
        self.assertFalse(policy.should_exclude_file('charlie'))

    def test_exclude_dir(self):
        policy = ScanPoliciesManager(exclude_dir_regexes=('alfa', 'bravo$'))
        self.assertTrue(policy.should_exclude_directory('alfa'))
        self.assertTrue(policy.should_exclude_directory('alfa2'))
        self.assertTrue(policy.should_exclude_directory('alfa/hello'))

        self.assertTrue(policy.should_exclude_directory('bravo'))
        self.assertFalse(policy.should_exclude_directory('bravo2'))
        self.assertFalse(policy.should_exclude_directory('bravo/hello'))

        self.assertTrue(policy.should_exclude_file('alfa/foo'))
        self.assertTrue(policy.should_exclude_file('alfa2/hello/foo'))
        self.assertTrue(policy.should_exclude_file('alfa/hello/foo.txt'))

        self.assertTrue(policy.should_exclude_file('bravo/foo'))
        self.assertFalse(policy.should_exclude_file('bravo2/hello/foo'))
        self.assertTrue(policy.should_exclude_file('bravo/hello/foo.txt'))

    def test_include_without_exclude(self):
        with self.assertRaises(InvalidArgument):
            ScanPoliciesManager(include_file_regexes=('.*',))
