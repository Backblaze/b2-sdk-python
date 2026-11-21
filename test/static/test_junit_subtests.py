######################################################################
#
# File: test/static/test_junit_subtests.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Exercise real pytest reports, including subtest outcomes and xdist delivery."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('workers', [0, 2])
@pytest.mark.parametrize('outcome', ['passed', 'failed', 'skipped'])
def test_junit_subtest_totals_match_individual_cases(tmp_path, workers, outcome):
    # Use the repository's plugin declarations, without loading its SDK fixtures
    # into the child fixture directory or inheriting external pytest options.
    declarations = (ROOT / 'test/conftest.py').read_text()
    line = next(line for line in declarations.splitlines() if line.startswith('pytest_plugins ='))
    (tmp_path / 'conftest.py').write_text(line + '\n')
    (tmp_path / 'pytest.ini').write_text('[pytest]\n')
    (tmp_path / 'test_sample.py').write_text(
        textwrap.dedent(f"""
        import unittest
        import pytest

        def check(i):
            if i == 1 and {outcome!r} == 'failed':
                assert False, 'deliberate subtest failure'
            if i == 1 and {outcome!r} == 'skipped':
                pytest.skip('deliberate subtest skip')

        class TestUnittest(unittest.TestCase):
            def test_subtests(self):
                for i in range(2):
                    with self.subTest(i=i):
                        check(i)

        def test_pytest(subtests):
            for i in range(2):
                with subtests.test(i=i):
                    check(i)

        def test_pass():
            assert True

        @pytest.mark.skip(reason='deliberate regular skip')
        def test_skip():
            pass

        @pytest.mark.xfail(strict=True, reason='deliberate expected failure')
        def test_xfail():
            assert False
    """)
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(('B2_', 'PYTEST_', 'COV_CORE_', 'COVERAGE_'))
    }
    env['PYTHONPATH'] = str(ROOT)
    completed = subprocess.run(
        [sys.executable, '-m', 'pytest', '-n', str(workers), '--junitxml=result.xml', '-q'],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert completed.returncode == (1 if outcome == 'failed' else 0), (
        completed.stdout + completed.stderr
    )
    suite = ET.parse(tmp_path / 'result.xml').getroot().find('testsuite')
    cases = suite.findall('testcase')
    assert int(suite.get('tests')) == len(cases), completed.stdout
    # Two parents, four independently reported subtests, and three plain tests.
    assert len(cases) == 9
    for tag, attribute in [('failure', 'failures'), ('error', 'errors'), ('skipped', 'skipped')]:
        assert int(suite.get(attribute)) == sum(case.find(tag) is not None for case in cases)
    assert all(
        sum(child.tag in ('failure', 'error', 'skipped') for child in case) <= 1 for case in cases
    )
    assert int(suite.get('failures')) == (3 if outcome == 'failed' else 0)
    assert int(suite.get('errors')) == 0
    assert int(suite.get('skipped')) == (4 if outcome == 'skipped' else 2)
