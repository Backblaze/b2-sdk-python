######################################################################
#
# File: test/junit_subtests.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Keep pytest 9's JUnit subtest counts consistent with its case elements.

Only the XML reporter sees copied subtest reports with distinct identities.
Other plugins, test execution and pytest's terminal/exit outcomes are unchanged.
This compatibility layer deliberately uses pytest's internal reporter API;
its subprocess regressions must pass when upgrading pytest.
"""

from __future__ import annotations

from copy import copy
from itertools import count

import pytest
from _pytest.junitxml import LogXML, xml_key
from _pytest.subtests import SubtestReport


class SubtestLogXML(LogXML):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subtest_ids = count(1)

    @pytest.hookimpl(trylast=True)
    def pytest_runtest_logreport(self, report):
        if not isinstance(report, SubtestReport):
            return super().pytest_runtest_logreport(report)
        report = copy(report)
        report.nodeid += f'::subtest-{next(self._subtest_ids)}'
        super().pytest_runtest_logreport(report)
        # The normal reporter writes output at teardown, which subtests lack.
        # Failure-only logging and regular skips have already written theirs.
        if (
            report.passed
            or (report.failed and self.log_passing_tests)
            or (report.skipped and hasattr(report, 'wasxfail'))
        ):
            self.node_reporter(report).write_captured_output(report)
        # Subtests have a call report, but no later teardown to close the case.
        self.finalize(report)
        self.open_reports = [entry for entry in self.open_reports if entry is not report]

    def pytest_sessionfinish(self):
        # Upstream subtracts a logical test when call and teardown both fail,
        # although it emits two XML cases. Compensate from the actual case list
        # so every outcome remains present and the declared count matches it.
        # An interrupted call can leave a bare, unfinished XML case. Do not
        # reconcile such a report: that would manufacture a passing outcome.
        if not self.node_reporters:
            self.cnt_double_fail_tests = sum(self.stats.values()) - len(self.node_reporters_ordered)
        super().pytest_sessionfinish()


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    original = config.stash.get(xml_key, None)
    if original is None:
        return  # No --junitxml, or an xdist worker: leave reporting untouched.
    replacement = SubtestLogXML(
        original.logfile,
        original.prefix,
        original.suite_name,
        original.logging,
        original.report_duration,
        original.family,
        original.log_passing_tests,
    )
    config.pluginmanager.unregister(original)
    config.stash[xml_key] = replacement
    config.pluginmanager.register(replacement)
