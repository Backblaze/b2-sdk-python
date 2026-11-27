######################################################################
#
# File: test/unit/test_b2_simulator_plugin.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Tests of the b2-simulator pytest plugin (``test/unit/b2_simulator.py``)."""

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


def run_pytest(tmp_path, test_source, *args, env_update=None, env_remove=(), unit=True):
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
            *(['-p', 'test.unit.conftest'] if unit else []),
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


def test_without_a_checkout_the_run_fails_and_names_the_variable(tmp_path):
    completed = run_pytest(tmp_path, PASSING_TEST, env_remove=('B2_SIMULATOR_DIR',))
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    assert 'the unit tests need B2_SIMULATOR_DIR' in completed.stderr
    assert '--rawsimulator' in completed.stderr
    assert 'passed' not in completed.stdout
    assert 'skipped' not in completed.stdout


def test_a_directory_that_is_not_a_checkout_fails(tmp_path):
    not_a_checkout = tmp_path / 'empty'
    not_a_checkout.mkdir()
    completed = run_pytest(
        tmp_path, PASSING_TEST, env_update={'B2_SIMULATOR_DIR': str(not_a_checkout)}
    )
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    expected = str(not_a_checkout / 'bin' / 'simulator' / 'serve.mjs')
    assert f'no b2-simulator server at {expected}' in completed.stderr
    assert 'passed' not in completed.stdout


def test_the_old_opt_in_flag_is_refused(tmp_path):
    completed = run_pytest(tmp_path, PASSING_TEST, '--b2-simulator')
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    assert '--b2-simulator' in completed.stderr


def test_with_rawsimulator_the_fixtures_use_raw_simulator(tmp_path):
    completed = run_pytest(
        tmp_path,
        """
        from apiver_deps import RawSimulator

        def test_fixture(b2api):
            assert type(b2api.session.raw_api) is RawSimulator
        """,
        '--rawsimulator',
        # No checkout is needed for the opt-out run.
        env_remove=('B2_SIMULATOR_DIR',),
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '1 passed' in completed.stdout


def test_with_rawsimulator_tests_that_need_b2_simulator_are_deselected(tmp_path):
    completed = run_pytest(
        tmp_path,
        """
        import pytest

        @pytest.mark.needs_b2_simulator
        def test_needs_server():
            raise AssertionError('must not run with --rawsimulator')

        def test_plain():
            pass
        """,
        '--rawsimulator',
        env_remove=('B2_SIMULATOR_DIR',),
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '1 passed, 1 deselected' in completed.stdout


def test_tests_outside_the_unit_suite_need_no_checkout(tmp_path):
    # Integration and static tests load the plugin through test/conftest.py only.
    completed = run_pytest(
        tmp_path,
        PASSING_TEST,
        env_remove=('B2_SIMULATOR_DIR',),
        unit=False,
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '1 passed' in completed.stdout


def test_the_whole_test_directory_still_collects():
    # pytest refuses `pytest_plugins` in a conftest below the top level when it collects `test`.
    completed = subprocess.run(
        [sys.executable, '-m', 'pytest', '--co', '-q', '-p', 'no:cacheprovider', 'test'],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout[-2000:]


@pytest.mark.needs_b2_simulator
def test_b2api_fixture_talks_to_this_workers_server(b2api, b2_simulator_server):
    assert b2api.account_info.get_api_url() == b2_simulator_server.api_url
    # The account id comes from the server, not from the key id the test logged in with.
    assert b2api.account_info.get_application_key_id() == 'sim-test-id'
    assert b2api.account_info.get_account_id() != 'sim-test-id'
    bucket = b2api.create_bucket('plugin-start-check', 'allPrivate')
    assert [b.name for b in b2api.list_buckets()] == [bucket.name]


def _part_sizes_from_the_server(b2api):
    # Read the server's answer: at apiver v0 and v1 the account info replaces the absolute
    # minimum part size with the SDK's own default, whatever the server says.
    from test.unit.b2_simulator import KEY, KEY_ID

    answer = b2api.session.raw_api.authorize_account('unused-realm', KEY_ID, KEY)
    storage = answer['apiInfo']['storageApi']
    return storage['absoluteMinimumPartSize'], storage['recommendedPartSize']


@pytest.mark.needs_b2_simulator
def test_reset_uses_raw_simulator_part_size_by_default(b2api):
    assert _part_sizes_from_the_server(b2api) == (
        RawSimulator.MIN_PART_SIZE,
        RawSimulator.MIN_PART_SIZE,
    )


@pytest.mark.needs_b2_simulator
@pytest.mark.b2_simulator_config(minimumPartSize=300, recommendedPartSize=400)
def test_marker_config_is_sent_with_the_reset(b2api):
    assert _part_sizes_from_the_server(b2api) == (300, 400)


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
def test_a_second_account_fails_instead_of_reusing_the_first(b2api):
    # RawSimulator.create_account() makes a new account on every call; b2-simulator has one.
    with pytest.raises(RuntimeError, match='b2-simulator has one account'):
        b2api.session.raw_api.create_account()


@pytest.mark.needs_b2_simulator
@pytest.mark.b2_simulator_config(extraAccounts=1)
def test_a_second_account_comes_from_extra_accounts(b2api):
    from apiver_deps import B2Api, B2HttpApiConfig, InMemoryAccountInfo

    from test.unit.b2_simulator import raw_api_class

    key_id, key = b2api.session.raw_api.create_account()
    assert (key_id, key) == ('sim-master-id-2', 'sim-master-secret-2')
    other = B2Api(
        InMemoryAccountInfo(),
        api_config=B2HttpApiConfig(_raw_api_class=raw_api_class(RawSimulator)),
    )
    other.authorize_account(application_key_id=key_id, application_key=key, realm='production')
    assert other.account_info.get_account_id() != b2api.account_info.get_account_id()
    # One extra account: a third one fails like any account the server does not have.
    with pytest.raises(RuntimeError, match='b2-simulator has 2 accounts'):
        b2api.session.raw_api.create_account()


@pytest.mark.needs_b2_simulator
def test_control_reaches_this_workers_control_plane(b2api):
    from test.unit import b2_simulator

    assert b2_simulator.enabled()
    b2api.list_buckets()
    entries = b2_simulator.control('GET', '/journal')['entries']
    assert [e['endpoint'] for e in entries][-1] == 'b2_list_buckets'


@pytest.mark.rawsimulator(reason='checks that a test kept on RawSimulator has no control plane')
def test_a_test_kept_on_raw_simulator_has_no_control_plane(b2api):
    from test.unit import b2_simulator

    assert not b2_simulator.enabled()
    assert type(b2api.session.raw_api) is RawSimulator
    # The first message is the default run's, the second the --rawsimulator run's.
    with pytest.raises(
        RuntimeError, match='not for a test kept on RawSimulator|not used with --rawsimulator'
    ):
        b2_simulator.control('GET', '/journal')


@pytest.mark.needs_b2_simulator
def test_a_marked_test_stays_on_raw_simulator_and_the_others_do_not(tmp_path):
    completed = run_pytest(
        tmp_path,
        """
        import pytest
        from apiver_deps import B2RawHTTPApi, RawSimulator

        @pytest.mark.rawsimulator(reason='Backblaze/b2-sdk-python#0 (example)')
        def test_marked(b2api):
            assert type(b2api.session.raw_api) is RawSimulator
            RawSimulator()  # building one directly is allowed too

        def test_unmarked(b2api):
            assert isinstance(b2api.session.raw_api, B2RawHTTPApi)
        """,
        '-rA',
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '2 passed' in completed.stdout


@pytest.mark.needs_b2_simulator
def test_the_marker_can_be_limited_to_api_versions(tmp_path, apiver_int):
    completed = run_pytest(
        tmp_path,
        f"""
        import pytest
        from apiver_deps import B2RawHTTPApi, RawSimulator

        @pytest.mark.rawsimulator(reason='example', to_ver={apiver_int - 1})
        def test_below(b2api):
            assert isinstance(b2api.session.raw_api, B2RawHTTPApi)

        @pytest.mark.rawsimulator(reason='example', from_ver={apiver_int})
        def test_from(b2api):
            assert type(b2api.session.raw_api) is RawSimulator
        """,
        f'--api=v{apiver_int}',
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '2 passed' in completed.stdout


@pytest.mark.needs_b2_simulator
@pytest.mark.parametrize(
    'marker,message',
    [
        (['@pytest.mark.rawsimulator'], 'needs a reason'),
        (["@pytest.mark.rawsimulator(reason='')"], 'needs a reason'),
        (
            ["@pytest.mark.rawsimulator(reason='x')", '@pytest.mark.needs_b2_simulator'],
            'cannot be marked both',
        ),
    ],
)
def test_a_wrong_marker_fails_the_test(tmp_path, marker, message):
    source = '\n'.join(['import pytest', '', *marker, 'def test_marked(b2api):', '    pass', ''])
    completed = run_pytest(tmp_path, source)
    assert completed.returncode == pytest.ExitCode.TESTS_FAILED, completed.stdout
    assert message in completed.stdout
    assert 'passed' not in completed.stdout.splitlines()[-1]


@pytest.mark.needs_b2_simulator
def test_an_unmarked_test_cannot_build_raw_simulator(tmp_path):
    completed = run_pytest(
        tmp_path,
        """
        from apiver_deps import RawSimulator

        def test_direct():
            RawSimulator()

        def test_subclass():
            class Patched(RawSimulator):
                pass

            Patched()
        """,
    )
    assert completed.returncode == pytest.ExitCode.TESTS_FAILED, completed.stdout
    assert '2 failed' in completed.stdout
    assert completed.stdout.count('@pytest.mark.rawsimulator(reason=') >= 2


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
