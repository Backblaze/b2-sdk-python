######################################################################
#
# File: test/unit/test_sdkharness_fail_not_skip.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os
import socket
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
LIB = ROOT / '.sdkharness' / 'tests' / 'lib'
CONFORMANCE = ROOT / '.sdkharness' / 'tests' / 'conformance'
LEAVES = ('files.upload', 'files.hide', 'keys.multi_bucket')


def load(name):
    spec = spec_from_file_location(f'sdkharness_{name}', LIB / f'{name}.py')
    module = module_from_spec(spec)
    sys.path.insert(0, str(LIB))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(LIB))
    return module


def dead_loopback_url():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    return f'http://127.0.0.1:{port}'  # the socket is closed again: nothing listens there


def run_leaf(leaf, simulator_url, extra_path=None):
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(('B2_', 'CONFORMANCE_', 'RESILIENCE_', 'SDKHARNESS_'))
    }
    python_path = [str(ROOT)]
    if extra_path:
        python_path.insert(0, str(extra_path))
    env.update(
        PYTHONPATH=os.pathsep.join(python_path),
        CONFORMANCE_TARGET='simulator',
        CONFORMANCE_SIMULATOR_URL=simulator_url,
    )
    return subprocess.run(
        [sys.executable, str(CONFORMANCE / leaf)],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def dispatcher_verdict(contract, leaf, completed):
    return contract.translate('conformance', leaf, completed.returncode, completed.stdout)


@pytest.mark.parametrize('leaf', LEAVES)
def test_dead_simulator_port_is_a_fail_not_a_skip(leaf):
    contract = load('contract')
    completed = run_leaf(leaf, dead_loopback_url())
    assert completed.returncode == 1, completed.stdout
    assert 'COULD-NOT-RUN' not in completed.stdout
    outcome, _reason, code = dispatcher_verdict(contract, leaf, completed)
    assert (outcome, code) == ('FAIL', 1)


@pytest.mark.parametrize('leaf', LEAVES)
def test_sdk_import_error_is_a_fail_not_a_skip(leaf, tmp_path):
    broken = tmp_path / 'b2sdk'
    broken.mkdir()
    (broken / '__init__.py').write_text('raise ImportError("circular import inside the SDK")\n')
    contract = load('contract')
    completed = run_leaf(leaf, dead_loopback_url(), extra_path=tmp_path)
    assert completed.returncode == 1, completed.stdout
    assert 'COULD-NOT-RUN' not in completed.stdout
    assert 'ImportError' in completed.stdout
    outcome, _reason, code = dispatcher_verdict(contract, leaf, completed)
    assert (outcome, code) == ('FAIL', 1)


@pytest.mark.parametrize('leaf', LEAVES)
def test_a_missing_b2sdk_module_is_still_could_not_run(leaf, tmp_path):
    # `import b2sdk` raising ModuleNotFoundError(name='b2sdk') is a genuinely missing runtime.
    missing = tmp_path / 'b2sdk'
    missing.mkdir()
    (missing / '__init__.py').write_text(
        "raise ModuleNotFoundError(\"No module named 'b2sdk'\", name='b2sdk')\n"
    )
    contract = load('contract')
    completed = run_leaf(leaf, dead_loopback_url(), extra_path=tmp_path)
    assert completed.returncode == 0, completed.stdout
    outcome, reason, code = dispatcher_verdict(contract, leaf, completed)
    assert (outcome, code) == ('SKIP', 0)
    assert reason.startswith('missing-runtime')


def test_missing_runtime_is_only_b2sdk_itself_not_found():
    guard = load('loopback_guard')
    assert guard.missing_runtime(ModuleNotFoundError('x', name='b2sdk'))
    assert not guard.missing_runtime(ModuleNotFoundError('x', name='b2sdk.v3.exception'))
    assert not guard.missing_runtime(ModuleNotFoundError('x', name='requests'))
    assert not guard.missing_runtime(ImportError('cannot import name'))


@pytest.mark.parametrize(
    'reason',
    [
        'unreachable -- B2ConnectionError at step authenticate',
        'unauthorized -- Unauthorized at step authenticate',
        'unauthorized -- AccessDenied at step create bucket',
        'unreachable -- ServiceError at step upload',
        'missing-runtime-ish -- unknown reason',
    ],
)
def test_dispatcher_turns_non_genuine_could_not_run_reasons_into_fail(reason):
    contract = load('contract')
    line = f'CONFORMANCE b2-sdk-python files.upload @simulator: COULD-NOT-RUN ({reason})\n'
    outcome, _reason, code = contract.translate('conformance', 'files.upload', 0, line)
    assert (outcome, code) == ('FAIL', 1)


@pytest.mark.parametrize('reason', ['no-realm-option', 'missing-runtime', 'no-client-option'])
def test_dispatcher_keeps_genuine_could_not_run_reasons_as_skip(reason):
    contract = load('contract')
    line = f'CONFORMANCE b2-sdk-python files.upload @simulator: COULD-NOT-RUN ({reason} -- why)\n'
    assert contract.translate('conformance', 'files.upload', 0, line) == (
        'SKIP',
        f'{reason} -- why',
        0,
    )


def test_child_environment_drops_proxies_and_pins_no_proxy():
    contract = load('contract')
    parent = {
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8180',
        'HTTP_PROXY': 'http://127.0.0.1:9',
        'https_proxy': 'http://127.0.0.1:9',
        'ALL_PROXY': 'socks5://127.0.0.1:9',
        'NO_PROXY': 'example.invalid',
    }
    child = contract.child_environment('conformance', parent)
    assert not {
        name for name in child if name.lower().endswith('_proxy') and 'no' not in name.lower()
    }
    assert child['NO_PROXY'] == child['no_proxy'] == '127.0.0.1'
    assert parent['HTTP_PROXY'] == 'http://127.0.0.1:9'  # the caller's mapping is untouched


def test_leaf_refusal_scrubs_the_proxy_environment(monkeypatch):
    guard = load('loopback_guard')
    monkeypatch.setenv('HTTP_PROXY', 'http://127.0.0.1:9')
    monkeypatch.setenv('https_proxy', 'http://127.0.0.1:9')
    monkeypatch.setenv('NO_PROXY', 'example.invalid')
    monkeypatch.delenv('CONFORMANCE_TARGET', raising=False)
    monkeypatch.setenv('CONFORMANCE_SIMULATOR_URL', 'http://127.0.0.1:8180')
    assert guard.refusal('conformance') is None
    assert 'HTTP_PROXY' not in os.environ and 'https_proxy' not in os.environ
    assert os.environ['NO_PROXY'] == '127.0.0.1'
