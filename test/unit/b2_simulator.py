######################################################################
#
# File: test/unit/b2_simulator.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Pytest plugin: run the unit tests against the standalone b2-simulator.

The unit tests run against b2-simulator by default. With ``B2_SIMULATOR_DIR`` set to a checkout
of backblaze-labs/b2-simulator, the plugin starts one simulator per pytest process (so one per
pytest-xdist worker) and resets it before every test. Tests that build their API through
:func:`raw_api_class` (the ``b2api`` fixture, ``TestCaseWithBucket`` and the other shared
setups) talk to it over HTTP instead of using the in-process ``RawSimulator``.
Without a usable checkout the run stops with a usage error; it never skips.

``@pytest.mark.rawsimulator(reason=...)`` keeps one test on the in-process ``RawSimulator``; the
reason names why (a ``RawSimulator`` self-test, or the issue that blocks the move). An unmarked
test cannot build a ``RawSimulator`` at all. ``pytest --rawsimulator`` runs the whole unit suite
on ``RawSimulator`` instead, and deselects the tests marked ``needs_b2_simulator``.

``@pytest.mark.b2_simulator_config(**settings)`` adds settings to the reset before that test
(keys of the simulator's ``POST /reset`` ``config``, for example ``minimumPartSize``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import typing
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pytest

ENV_VAR = 'B2_SIMULATOR_DIR'
SERVER_SCRIPT = Path('bin', 'simulator', 'serve.mjs')
START_TIMEOUT_SECONDS = 30
# The simulator's fixed full-access credential (see its docs/CONTRACT.md).
KEY_ID = 'sim-test-id'
KEY = 'sim-test-secret'
# RawSimulator.MIN_PART_SIZE, which most part-size tests are written for.
PART_SIZE = 200
DEFAULT_CONFIG = {'minimumPartSize': PART_SIZE, 'recommendedPartSize': PART_SIZE}
# Exits node when its stdin closes, which happens when the process that started it dies in any
# way (including SIGKILL), so a simulator never outlives its pytest worker.
_EXIT_WITH_PARENT = (
    'data:text/javascript,process.stdin.on("end",()=>process.exit(0));process.stdin.resume()'
)

# Reasons for @pytest.mark.rawsimulator that several tests share.
#
# SDK defects that only an HTTP server exposes: RawSimulator accepts these calls, B2 does not.
COPY_SSE_B2_SOURCE_DEFECT = (
    'Backblaze/b2-sdk-python#772: B2RawHTTPApi.copy_file asserts that a source encryption is '
    'SSE-C, so the SSE-B2 source subtests fail in the SDK before any request is sent'
)
V1_COPY_FILE_INFO_DEFECT = (
    'Backblaze/b2-sdk-python#773: v1 Bucket.copy_file sends fileInfo {} with the COPY metadata '
    'directive, which B2 refuses'
)
SIMPLE_DOWNLOAD_RESUME_DEFECT = (
    'Backblaze/b2-sdk-python#776: SimpleDownloader does not resume after a connection drops '
    'mid-body (ChunkedEncodingError over HTTP); it resumes only after a body that ends cleanly '
    'short, which only RawSimulator produces'
)
# A b2-simulator gap. apiver v0 to v2 authorize at
# B2 API v3 with keys that b2_create_key (v3) restricted to one bucket. Real B2 serves such a key
# at v3 (measured on 2026-11-27); b2-simulator v0.6.0 refuses every bucket-restricted key at v3.
# Fixing that is a b2-simulator change.
V3_BUCKET_KEY_GAP = (
    'b2-simulator v0.6.0 refuses a single-bucket key made by b2_create_key v3 at '
    'b2_authorize_account v3; real B2 accepts it'
)

UNIT_TEST_DIR = Path(__file__).parent
OPT_OUT = '--rawsimulator'

_server: SimulatorServer | None = None
# Whether this run uses b2-simulator (a unit-test run without --rawsimulator).
_enabled = False
# Whether the running test is marked rawsimulator, so it stays on RawSimulator.
_test_on_raw_simulator = False
# The settings of the last reset: create_account() reads extraAccounts from them.
_config: dict = {}


@dataclass
class SimulatorServer:
    process: subprocess.Popen
    api_url: str
    control_url: str
    stderr: typing.IO[bytes]

    @classmethod
    def start(cls, simulator_dir) -> SimulatorServer:
        script = Path(simulator_dir) / SERVER_SCRIPT
        args = ['--control', '--profile', 'b2']
        args += ['--min-part-size', str(PART_SIZE), '--recommended-part-size', str(PART_SIZE)]
        stderr = tempfile.TemporaryFile()
        process = subprocess.Popen(
            [_node(), '--import', _EXIT_WITH_PARENT, str(script), *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
        )
        lines = _read_ready_lines(process)
        urls = dict(line.split(' ', 1) for line in lines if ' ' in line)
        api_url = next(
            (line.split()[1] for line in lines if line.startswith('SIMULATOR-LISTENING http:')),
            None,
        )
        if api_url is None or 'SIMULATOR-CONTROL' not in urls:
            process.kill()
            process.wait()
            process.stdout.close()
            stderr.seek(0)
            message = stderr.read().decode(errors='replace')
            stderr.close()
            raise RuntimeError(
                f'b2-simulator at {script} did not start (exit code {process.returncode}): '
                f'stdout {lines!r}, stderr {message!r}'
            )
        return cls(process, api_url, urls['SIMULATOR-CONTROL'], stderr)

    def reset(self, config=None):
        global _config
        _config = {**DEFAULT_CONFIG, **(config or {})}
        self.control('POST', '/reset', {'config': _config})

    def control(self, method, path, body=None):
        """Send one control-plane request (see the simulator's docs/CONTRACT.md); return its JSON."""
        request = urllib.request.Request(
            self.control_url + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={'Content-Type': 'application/json'},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read()
        return json.loads(text) if text else {}

    def stop(self):
        try:
            # The simulator exits when its stdin closes; terminate and kill are fallbacks.
            self.process.stdin.close()
            for fallback in (self.process.terminate, self.process.kill):
                try:
                    self.process.wait(timeout=5)
                    break
                except subprocess.TimeoutExpired:
                    fallback()
            self.process.wait(timeout=5)
        finally:
            self.process.stdout.close()
            self.stderr.close()


def _node():
    node = shutil.which('node')
    if node is None:
        raise pytest.UsageError(
            f'the unit tests need Node.js 20.6 or later on PATH (`node`) for b2-simulator; '
            f'use {OPT_OUT} to run them on RawSimulator instead'
        )
    return node


def _read_ready_lines(process, count=3):
    """Read the simulator's ready lines; give up after START_TIMEOUT_SECONDS or at end of output."""
    lines = []

    def read():
        for _ in range(count):
            line = process.stdout.readline()
            if not line:
                return
            lines.append(line.strip())

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(START_TIMEOUT_SECONDS)
    return list(lines)


def simulator_dir_from_env() -> Path:
    value = os.environ.get(ENV_VAR)
    if not value:
        raise pytest.UsageError(
            f'the unit tests need {ENV_VAR}: set it to a checkout of '
            f'backblaze-labs/b2-simulator, or use {OPT_OUT} to run them on RawSimulator'
        )
    script = Path(value) / SERVER_SCRIPT
    if not script.is_file():
        raise pytest.UsageError(f'no b2-simulator server at {script} (check {ENV_VAR})')
    return Path(value)


def enabled() -> bool:
    """Whether the running test uses b2-simulator (not ``--rawsimulator``, not marked)."""
    return _enabled and not _test_on_raw_simulator


def control(method, path, body=None):
    """Send a request to this worker's b2-simulator control plane; only on b2-simulator."""
    if not _enabled:
        raise RuntimeError(f'the b2-simulator control plane is not used with {OPT_OUT}')
    if _test_on_raw_simulator:
        raise RuntimeError('the b2-simulator control plane is not for a test kept on RawSimulator')
    if _server is None:
        raise RuntimeError('b2-simulator is not running (use it inside a test)')
    return _server.control(method, path, body)


def raw_api_class(default):
    """
    Return the raw API class a test should build its ``B2Api`` with.

    ``default`` is ``RawSimulator`` or a test's subclass of it. On b2-simulator the SDK's own
    ``RawSimulator`` is replaced by an HTTP client of the per-worker b2-simulator; a test's
    subclass (which overrides simulator internals) is returned unchanged, and only a test marked
    ``rawsimulator`` can build it.
    """
    if not enabled() or not default.__module__.startswith('b2sdk.'):
        return default
    return _http_raw_api_class()


_HTTP_CLASS = None


def _http_raw_api_class():
    global _HTTP_CLASS
    if _HTTP_CLASS is None:
        from apiver_deps import B2Http, B2RawHTTPApi

        class B2SimulatorRawApi(B2RawHTTPApi):
            """``B2RawHTTPApi`` for this worker's b2-simulator, with RawSimulator's test hooks."""

            MIN_PART_SIZE = PART_SIZE

            def __init__(self, b2_http=None):
                super().__init__(b2_http or B2Http())

            def create_account(self):
                """
                Return the simulator's fixed credential; its key id is not the account id.

                RawSimulator makes a new account on every call. b2-simulator has one account, plus
                the ``extraAccounts`` of the reset (``@pytest.mark.b2_simulator_config``): later calls
                return their master credentials in turn, and a call past the last account fails
                instead of silently returning an account again.
                """
                count = getattr(self, '_accounts_created', 0)
                extra = _config.get('extraAccounts', 0)
                if count > extra:
                    if extra == 0:
                        raise RuntimeError(
                            'b2-simulator has one account: this test needs a second account; '
                            'add @pytest.mark.b2_simulator_config(extraAccounts=N)'
                        )
                    raise RuntimeError(f'b2-simulator has {extra + 1} accounts (extraAccounts)')
                self._accounts_created = count + 1
                if count == 0:
                    return KEY_ID, KEY
                # Accounts 2 to N + 1 (docs/CONTRACT.md, "More accounts").
                return f'sim-master-id-{count + 1}', f'sim-master-secret-{count + 1}'

            def authorize_account(self, realm_url, application_key_id, application_key):
                # Every realm goes to the simulator, so a test can never reach real B2.
                if _server is None:
                    raise RuntimeError('b2-simulator is not running (use it inside a test)')
                return super().authorize_account(
                    _server.api_url, application_key_id, application_key
                )

        _HTTP_CLASS = B2SimulatorRawApi
    return _HTTP_CLASS


def pytest_addoption(parser):
    parser.addoption(
        OPT_OUT,
        action='store_true',
        default=False,
        help='run the unit tests on the in-process RawSimulator instead of b2-simulator '
        f'(by default they need a b2-simulator checkout in ${ENV_VAR})',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'b2_simulator_config(**settings): b2-simulator reset settings for this test',
    )
    config.addinivalue_line(
        'markers',
        f'needs_b2_simulator: run only on b2-simulator (deselected with {OPT_OUT})',
    )
    config.addinivalue_line(
        'markers',
        'rawsimulator(reason, *, from_ver=0, to_ver=sys.maxsize): keep this test on the '
        'in-process RawSimulator at these apivers; the reason names the issue or the cause',
    )


def activate(config):
    """
    Use b2-simulator for this run unless ``--rawsimulator`` is given.

    Called by the unit tests' conftest, so integration and static runs never need a checkout.
    """
    global _enabled
    if config.getoption(OPT_OUT):
        return
    simulator_dir_from_env()
    _node()
    _enabled = True


def pytest_report_header(config):
    if _enabled:
        return f'b2-simulator: {os.environ[ENV_VAR]}'
    if config.getoption(OPT_OUT):
        return f'b2-simulator: not used ({OPT_OUT})'


def pytest_collection_modifyitems(config, items):
    if not config.getoption(OPT_OUT):
        return
    deselected = [item for item in items if item.get_closest_marker('needs_b2_simulator')]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = [item for item in items if not item.get_closest_marker('needs_b2_simulator')]


def _kept_on_raw_simulator(item) -> bool:
    """Whether a ``rawsimulator`` marker keeps this test on RawSimulator at this apiver."""
    marker = item.get_closest_marker('rawsimulator')
    if marker is None:
        return False
    if not marker.kwargs.get('reason'):
        pytest.fail(
            '@pytest.mark.rawsimulator needs a reason=... naming why the test stays on '
            'RawSimulator (the issue that blocks it, or that it tests RawSimulator itself)',
            pytrace=False,
        )
    if item.get_closest_marker('needs_b2_simulator'):
        pytest.fail(
            'a test cannot be marked both rawsimulator and needs_b2_simulator', pytrace=False
        )
    apiver = int(item.config.getoption('--api')[1:])
    from_ver = marker.kwargs.get('from_ver', 0)
    to_ver = marker.kwargs.get('to_ver', sys.maxsize)
    return from_ver <= apiver <= to_ver


def _refuse_raw_simulator(monkeypatch):
    """Make building any RawSimulator fail, so no unmarked test can use one unnoticed."""
    from b2sdk._internal.raw_simulator import RawSimulator

    def refuse(self, *args, **kwargs):
        raise RuntimeError(
            f'{type(self).__name__} is the in-process simulator; the unit tests run on '
            'b2-simulator. Build the API through raw_api_class(), or keep this test on it with '
            '@pytest.mark.rawsimulator(reason=...) naming why'
        )

    monkeypatch.setattr(RawSimulator, '__init__', refuse)


@pytest.fixture(scope='session')
def b2_simulator_server():
    """This process's b2-simulator, started on first use and stopped with the session."""
    global _server
    if not _enabled:
        raise pytest.UsageError(f'the b2_simulator_server fixture is not used with {OPT_OUT}')
    _server = SimulatorServer.start(simulator_dir_from_env())
    yield _server
    _server.stop()
    _server = None


def _is_other_test_suite(path) -> bool:
    """Whether a test is in this repository's integration or static suites, not the unit tests."""
    parents = Path(path).parents
    return UNIT_TEST_DIR.parent in parents and UNIT_TEST_DIR not in parents


@pytest.fixture(autouse=True)
def _b2_simulator_reset(request, monkeypatch):
    """
    Before each unit test: reset the simulator with the test's ``b2_simulator_config``
    settings, or, for a test marked ``rawsimulator``, let it use RawSimulator.
    """
    global _test_on_raw_simulator
    if not _enabled or _is_other_test_suite(request.node.path):
        yield
        return
    if _kept_on_raw_simulator(request.node):
        _test_on_raw_simulator = True
        try:
            yield
        finally:
            _test_on_raw_simulator = False
        return
    _refuse_raw_simulator(monkeypatch)
    server = request.getfixturevalue('b2_simulator_server')
    config = {}
    for marker in reversed(list(request.node.iter_markers('b2_simulator_config'))):
        config.update(marker.kwargs)
    server.reset(config)
    yield
