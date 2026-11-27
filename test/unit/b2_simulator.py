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
Opt-in pytest plugin: run the unit tests against the standalone b2-simulator.

``pytest --b2-simulator`` with ``B2_SIMULATOR_DIR`` set to a checkout of
backblaze-labs/b2-simulator starts one simulator per pytest process (so one
per pytest-xdist worker) and resets it before every test. Tests that build their API through
:func:`raw_api_class` (the ``b2api`` fixture, ``TestCaseWithBucket`` and the other shared
setups) then talk to it over HTTP instead of using the in-process ``RawSimulator``.

Without ``--b2-simulator`` nothing changes: :func:`raw_api_class` returns the class it is given.
With ``--b2-simulator`` and no usable checkout, the run stops with a usage error; it never skips.

``@pytest.mark.b2_simulator_config(**settings)`` adds settings to the reset before that test
(keys of the simulator's ``POST /reset`` ``config``, for example ``minimumPartSize``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
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

_server: SimulatorServer | None = None
_enabled = False
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
        raise pytest.UsageError('--b2-simulator needs Node.js 20.6 or later on PATH (`node`)')
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
            f'--b2-simulator needs {ENV_VAR}: set it to a checkout of '
            'backblaze-labs/b2-simulator'
        )
    script = Path(value) / SERVER_SCRIPT
    if not script.is_file():
        raise pytest.UsageError(
            f'--b2-simulator: no b2-simulator server at {script} (check {ENV_VAR})'
        )
    return Path(value)


def enabled() -> bool:
    """Whether this run uses b2-simulator (``--b2-simulator``)."""
    return _enabled


def control(method, path, body=None):
    """Send a request to this worker's b2-simulator control plane; only with ``--b2-simulator``."""
    if not _enabled:
        raise RuntimeError('the b2-simulator control plane needs --b2-simulator')
    if _server is None:
        raise RuntimeError('b2-simulator is not running (use it inside a test)')
    return _server.control(method, path, body)


def raw_api_class(default):
    """
    Return the raw API class a test should build its ``B2Api`` with.

    ``default`` is ``RawSimulator`` or a test's subclass of it. Under ``--b2-simulator`` the SDK's
    own ``RawSimulator`` is replaced by an HTTP client of the per-worker b2-simulator; a test's
    subclass (which overrides simulator internals) is returned unchanged.
    """
    if not _enabled or not default.__module__.startswith('b2sdk.'):
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
        '--b2-simulator',
        action='store_true',
        default=False,
        help=f'run simulator-backed unit tests against b2-simulator (checkout in ${ENV_VAR})',
    )


def pytest_configure(config):
    global _enabled
    config.addinivalue_line(
        'markers',
        'b2_simulator_config(**settings): b2-simulator reset settings for this test '
        '(only used with --b2-simulator)',
    )
    config.addinivalue_line(
        'markers', 'needs_b2_simulator: run only with --b2-simulator (deselected otherwise)'
    )
    if config.getoption('--b2-simulator'):
        simulator_dir_from_env()
        _node()
        _enabled = True


def pytest_report_header(config):
    if config.getoption('--b2-simulator'):
        return f'b2-simulator: {os.environ[ENV_VAR]}'


def pytest_collection_modifyitems(config, items):
    if config.getoption('--b2-simulator'):
        return
    deselected = [item for item in items if item.get_closest_marker('needs_b2_simulator')]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = [item for item in items if not item.get_closest_marker('needs_b2_simulator')]


@pytest.fixture(scope='session')
def b2_simulator_server(pytestconfig):
    """This process's b2-simulator, started on first use and stopped with the session."""
    global _server
    if not pytestconfig.getoption('--b2-simulator'):
        raise pytest.UsageError('the b2_simulator_server fixture needs --b2-simulator')
    _server = SimulatorServer.start(simulator_dir_from_env())
    yield _server
    _server.stop()
    _server = None


@pytest.fixture(autouse=True)
def _b2_simulator_reset(request, pytestconfig):
    """Reset the simulator before each test, with the test's ``b2_simulator_config`` settings."""
    if not pytestconfig.getoption('--b2-simulator'):
        return
    server = request.getfixturevalue('b2_simulator_server')
    config = {}
    for marker in reversed(list(request.node.iter_markers('b2_simulator_config'))):
        config.update(marker.kwargs)
    server.reset(config)
