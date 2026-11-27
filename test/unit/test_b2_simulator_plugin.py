######################################################################
#
# File: test/unit/test_b2_simulator_plugin.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Tests of the opt-in ``--b2-simulator`` pytest plugin (``test/unit/b2_simulator.py``)."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
from apiver_deps import RawSimulator

ROOT = Path(__file__).parents[2]


def run_pytest(tmp_path, test_source, *args, env_update=None, env_remove=()):
    """Run pytest in a subprocess on one generated test file, with the unit-test plugins loaded."""
    test_file = tmp_path / 'test_generated.py'
    test_file.write_text(textwrap.dedent(test_source))
    env = {name: value for name, value in os.environ.items() if name not in env_remove}
    env.pop('PYTEST_XDIST_WORKER', None)
    env['PYTHONPATH'] = os.pathsep.join([str(ROOT), env.get('PYTHONPATH', '')])
    env.update(env_update or {})
    return subprocess.run(
        [
            sys.executable,
            '-m',
            'pytest',
            '-p',
            'test.conftest',
            '-p',
            'test.unit.conftest',
            '-p',
            'no:cacheprovider',
            '-p',
            'no:xdist',
            '-q',
            f'--rootdir={tmp_path}',
            *args,
            str(test_file),
        ],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


PASSING_TEST = """
def test_nothing():
    pass
"""


def test_opt_in_without_a_checkout_fails_and_names_the_variable(tmp_path):
    completed = run_pytest(
        tmp_path, PASSING_TEST, '--b2-simulator', env_remove=('B2_SIMULATOR_DIR',)
    )
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    assert '--b2-simulator needs B2_SIMULATOR_DIR' in completed.stderr
    assert 'passed' not in completed.stdout
    assert 'skipped' not in completed.stdout


def test_opt_in_with_a_directory_that_is_not_a_checkout_fails(tmp_path):
    not_a_checkout = tmp_path / 'empty'
    not_a_checkout.mkdir()
    completed = run_pytest(
        tmp_path,
        PASSING_TEST,
        '--b2-simulator',
        env_update={'B2_SIMULATOR_DIR': str(not_a_checkout)},
    )
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    expected = str(not_a_checkout / 'bin' / 'simulator' / 'serve.mjs')
    assert f'no b2-simulator server at {expected}' in completed.stderr
    assert 'passed' not in completed.stdout


def test_without_the_flag_the_fixtures_use_raw_simulator(tmp_path):
    completed = run_pytest(
        tmp_path,
        """
        from apiver_deps import RawSimulator

        def test_fixture(b2api):
            assert type(b2api.session.raw_api) is RawSimulator
        """,
        # Even with a checkout configured, nothing changes without the flag.
        env_update={'B2_SIMULATOR_DIR': str(tmp_path / 'unused')},
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '1 passed' in completed.stdout


@pytest.mark.needs_b2_simulator
def test_b2api_fixture_talks_to_this_workers_server(b2api, b2_simulator_server):
    assert b2api.account_info.get_api_url() == b2_simulator_server.api_url
    # The account id comes from the server, not from the key id the test logged in with.
    key_id, _ = b2api.session.raw_api.create_account()
    assert b2api.account_info.get_account_id() != key_id
    bucket = b2api.create_bucket('plugin-start-check', 'allPrivate')
    assert [b.name for b in b2api.list_buckets()] == [bucket.name]


@pytest.mark.needs_b2_simulator
def test_reset_uses_raw_simulator_part_size_by_default(b2api):
    assert b2api.account_info.get_absolute_minimum_part_size() == RawSimulator.MIN_PART_SIZE
    assert b2api.account_info.get_recommended_part_size() == RawSimulator.MIN_PART_SIZE


@pytest.mark.needs_b2_simulator
@pytest.mark.b2_simulator_config(minimumPartSize=300, recommendedPartSize=400)
def test_marker_config_is_sent_with_the_reset(b2api):
    assert b2api.account_info.get_absolute_minimum_part_size() == 300
    assert b2api.account_info.get_recommended_part_size() == 400


@pytest.mark.needs_b2_simulator
def test_each_test_starts_from_an_empty_server(tmp_path):
    # Both tests create the same bucket name; the second can only pass if the server was reset.
    completed = run_pytest(
        tmp_path,
        """
        import pytest

        @pytest.mark.parametrize('attempt', [1, 2])
        def test_create(b2api, b2_simulator_server, attempt, tmp_path_factory):
            b2api.create_bucket('reset-check', 'allPrivate')
            assert [b.name for b in b2api.list_buckets()] == ['reset-check']
            urls = tmp_path_factory.getbasetemp().parent / 'urls.txt'
            with open(urls, 'a') as f:
                f.write(b2_simulator_server.control_url + '\\n')
        """,
        '--b2-simulator',
        f'--basetemp={tmp_path / "base"}',
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '2 passed' in completed.stdout
    urls = (tmp_path / 'urls.txt').read_text().split()
    assert len(urls) == 2 and urls[0] == urls[1], 'one server per pytest process'


def _assert_on_simulator(case, server):
    from apiver_deps import B2RawHTTPApi

    case.setUp()
    assert isinstance(case.simulator, B2RawHTTPApi)
    assert case.api.account_info.get_api_url() == server.api_url
    assert [b.name for b in case.api.list_buckets()] == [case.bucket_name]


@pytest.mark.needs_b2_simulator
@pytest.mark.apiver(from_ver=1)
def test_test_case_with_bucket_uses_the_simulator(b2_simulator_server):
    from test.unit.bucket.test_bucket import TestCaseWithBucket

    _assert_on_simulator(TestCaseWithBucket('setUp'), b2_simulator_server)


@pytest.mark.needs_b2_simulator
@pytest.mark.apiver(0)
def test_v0_test_case_with_bucket_uses_the_simulator(b2_simulator_server):
    from test.unit.v0.test_bucket import TestCaseWithBucket

    _assert_on_simulator(TestCaseWithBucket('setUp'), b2_simulator_server)


@pytest.mark.needs_b2_simulator
def test_a_tests_own_simulator_subclass_stays_in_process():
    from test.unit.b2_simulator import raw_api_class

    class PatchedSimulator(RawSimulator):
        pass

    assert raw_api_class(PatchedSimulator) is PatchedSimulator


def _port_open(url):
    host, port = url.rsplit('/', 1)[-1].rsplit(':', 1)
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, int(port))) == 0


@pytest.mark.needs_b2_simulator
def test_server_stops_when_its_pytest_process_is_killed():
    child = subprocess.Popen(
        [
            sys.executable,
            '-c',
            'import sys, time\n'
            'from test.unit.b2_simulator import SimulatorServer\n'
            'server = SimulatorServer.start(sys.argv[1])\n'
            'print(server.control_url, flush=True)\n'
            'time.sleep(120)\n',
            os.environ['B2_SIMULATOR_DIR'],
        ],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        control_url = child.stdout.readline().strip()
        assert control_url.startswith('http://127.0.0.1:'), control_url
        assert _port_open(control_url)
        if hasattr(signal, 'SIGKILL'):
            child.send_signal(signal.SIGKILL)  # no atexit, no fixture teardown
        else:
            child.kill()
        child.wait(timeout=10)
        deadline = time.monotonic() + 10
        while _port_open(control_url) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _port_open(control_url), 'the simulator outlived the process that started it'
    finally:
        child.kill()
        child.stdout.close()
