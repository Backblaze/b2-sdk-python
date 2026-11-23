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


@pytest.mark.parametrize('workers', [0, 2])
def test_junit_totals_include_a_failed_parent_and_its_teardown_error(tmp_path, workers):
    (tmp_path / 'pytest.ini').write_text('[pytest]\n')
    (tmp_path / 'conftest.py').write_text("pytest_plugins = ['test.junit_subtests']\n")
    (tmp_path / 'test_sample.py').write_text(
        textwrap.dedent("""
        import pytest

        @pytest.fixture
        def broken_teardown():
            yield
            raise RuntimeError('deliberate teardown error')

        def test_subtest(subtests, broken_teardown):
            with subtests.test('deliberate failure'):
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
    assert completed.returncode == 1, completed.stdout + completed.stderr
    suite = ET.parse(tmp_path / 'result.xml').getroot().find('testsuite')
    cases = suite.findall('testcase')
    assert int(suite.get('tests')) == len(cases)
    assert len(cases) == 3  # failed subtest, failed parent call, errored teardown
    assert int(suite.get('failures')) == 2
    assert int(suite.get('errors')) == 1
    assert sum(case.find('failure') is not None for case in cases) == 2
    assert sum(case.find('error') is not None for case in cases) == 1


@pytest.mark.parametrize('workers', [0, 2])
@pytest.mark.parametrize('outcome', ['passed', 'failed', 'skipped', 'xfail'])
@pytest.mark.parametrize('log_passing', [True, False])
def test_junit_subtests_retain_requested_captured_output(tmp_path, workers, outcome, log_passing):
    (tmp_path / 'pytest.ini').write_text('[pytest]\n')
    (tmp_path / 'conftest.py').write_text("pytest_plugins = ['test.junit_subtests']\n")
    (tmp_path / 'test_sample.py').write_text(
        textwrap.dedent(f"""
        import sys
        import pytest

        def test_subtest(subtests):
            with subtests.test('captured output'):
                print('SUBTEST_STDOUT')
                print('SUBTEST_STDERR', file=sys.stderr)
                if {outcome!r} == 'failed':
                    assert False
                if {outcome!r} == 'skipped':
                    pytest.skip('deliberate skip')
                if {outcome!r} == 'xfail':
                    pytest.xfail('deliberate expected failure')
    """)
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(('B2_', 'PYTEST_', 'COV_CORE_', 'COVERAGE_'))
    }
    env['PYTHONPATH'] = str(ROOT)
    completed = subprocess.run(
        [
            sys.executable,
            '-m',
            'pytest',
            '-n',
            str(workers),
            '--junitxml=result.xml',
            '-q',
            '-o',
            'junit_logging=all',
            '-o',
            f'junit_log_passing_tests={log_passing}',
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert completed.returncode == (1 if outcome == 'failed' else 0), (
        completed.stdout + completed.stderr
    )
    cases = ET.parse(tmp_path / 'result.xml').getroot().findall('.//testcase')
    subcase = next(case for case in cases if case.get('name').startswith('subtest-'))
    for tag, token in [('system-out', 'SUBTEST_STDOUT'), ('system-err', 'SUBTEST_STDERR')]:
        captured = subcase.findall(tag)
        if log_passing or outcome != 'passed':
            assert len(captured) == 1
            assert token in captured[0].text
        else:
            assert not captured


def test_junit_does_not_count_an_interrupted_test_as_passed(tmp_path):
    (tmp_path / 'pytest.ini').write_text('[pytest]\n')
    (tmp_path / 'conftest.py').write_text("pytest_plugins = ['test.junit_subtests']\n")
    (tmp_path / 'test_sample.py').write_text(
        'def test_interrupted():\n    raise KeyboardInterrupt\n'
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(('B2_', 'PYTEST_', 'COV_CORE_', 'COVERAGE_'))
    }
    env['PYTHONPATH'] = str(ROOT)
    completed = subprocess.run(
        [sys.executable, '-m', 'pytest', '--junitxml=result.xml', '-q'],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 2, completed.stdout + completed.stderr
    suite = ET.parse(tmp_path / 'result.xml').getroot().find('testsuite')
    declared_passes = int(suite.get('tests')) - sum(
        int(suite.get(key)) for key in ('failures', 'errors', 'skipped')
    )
    assert declared_passes == 0, 'an interrupted call has no passing test outcome'
