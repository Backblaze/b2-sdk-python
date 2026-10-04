######################################################################
#
# File: test/unit/test_sdkharness_conformance_resilience_contract.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

MODULE = Path(__file__).parents[2] / '.sdkharness' / 'tests' / 'lib' / 'contract.py'


def load_contract():
    spec = spec_from_file_location('sdkharness_contract', MODULE)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def environment(level='conformance', scenario='files.upload'):
    result = {
        'SDKHARNESS_TEST_LEVEL': level,
        'SDKHARNESS_SCENARIO': scenario,
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8180',
    }
    if level == 'resilience':
        result['SDKHARNESS_SIMULATOR_CONTROL_URL'] = 'http://127.0.0.1:8181'
    return result


@pytest.mark.parametrize(
    'url',
    [
        'https://127.0.0.1:8180/',
        'http://localhost:8180/',
        'http://127.0.0.1/',
        'http://user@127.0.0.1:8180/',
        'http://127.0.0.1:8180/not-root',
        'http://127.0.0.2:8180/',
    ],
)
def test_rejects_unsafe_simulator_origins(url):
    contract = load_contract()
    values = environment()
    values['SDKHARNESS_SIMULATOR_URL'] = url
    with pytest.raises(contract.ContractFailure, match='configuration'):
        contract.validate_environment('conformance', values)


def test_requires_matching_level_and_safe_scenario():
    contract = load_contract()
    with pytest.raises(contract.ContractFailure, match='unexpected test level'):
        contract.validate_environment('resilience', environment())
    with pytest.raises(contract.ContractFailure, match='invalid scenario'):
        contract.validate_environment('conformance', environment(scenario='../escape'))


def test_translates_pass_fail_skip_and_rejects_ambiguous_output(capsys):
    contract = load_contract()
    prefix = 'CONFORMANCE b2-sdk-python files.upload @simulator: '
    assert contract.translate('conformance', 'files.upload', 0, prefix + 'PASS\n') == (
        'PASS',
        '-',
        0,
    )
    assert contract.translate(
        'conformance', 'files.upload', 1, prefix + 'FAIL (upload -- AssertionError)\n'
    ) == ('FAIL', 'upload -- AssertionError', 1)
    assert contract.translate(
        'conformance',
        'files.upload',
        0,
        prefix + 'COULD-NOT-RUN (missing-runtime -- unavailable)\n',
    ) == ('SKIP', 'missing-runtime -- unavailable', 0)
    assert contract.translate(
        'conformance', 'files.upload', 0, prefix + 'PASS\n' + prefix + 'PASS\n'
    ) == ('FAIL', 'contract: expected one standing result, got 2', 1)
    assert capsys.readouterr().out == ''


def test_result_is_exactly_five_fields_and_sanitizes_reason(capsys):
    contract = load_contract()
    contract.result('resilience', 'upload.retry_503', 'FAIL', 'step\tsecret\nnext')
    assert capsys.readouterr().out == (
        'SDKHARNESS_RESULT\tresilience\tupload.retry_503\tFAIL\tstep secret next\n'
    )


def test_child_environment_scrubs_real_b2_credentials_and_pins_checkout():
    contract = load_contract()
    values = environment()
    values['B2_APPLICATION_KEY_ID'] = 'real-key-id'
    values['B2_APPLICATION_KEY'] = 'real-secret'
    values['B2_OPERATOR_EXTRA'] = 'must-not-pass'

    child = contract.child_environment('conformance', values)

    assert child['B2_APPLICATION_KEY_ID'] == 'test-key-id'
    assert child['B2_APPLICATION_KEY'] == 'test-key'
    assert 'B2_OPERATOR_EXTRA' not in child
    assert child['PYTHONPATH'] == str(contract.REPOSITORY_ROOT)


LEAF_ROOT = Path(__file__).parents[2] / '.sdkharness' / 'tests'
LEAVES = sorted(
    path
    for level in ('conformance', 'resilience')
    for path in (LEAF_ROOT / level).iterdir()
    if path.is_file() and not path.name.startswith(('__', '.'))
)


def test_leaves_are_regular_files_only():
    assert LEAVES
    assert all(path.is_file() and not path.name.startswith('__') for path in LEAVES)


def run_leaf(path, extra_environment):
    import os
    import subprocess
    import sys

    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(('B2_', 'CONFORMANCE_', 'RESILIENCE_', 'SDKHARNESS_'))
    }
    env.update(extra_environment)
    return subprocess.run(
        [sys.executable, str(path)],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.parametrize('leaf', LEAVES, ids=lambda path: f'{path.parent.name}/{path.name}')
def test_leaf_checks_refuse_ambient_staging_runs(leaf):
    level = leaf.parent.name
    prefix = level.upper()
    ambient = {
        'B2_APPLICATION_KEY_ID': 'real-looking-key-id',
        'B2_APPLICATION_KEY': 'real-looking-key',
        f'{prefix}_TARGET': 'staging',
    }
    refused = run_leaf(leaf, ambient)
    assert refused.returncode == 1
    assert 'FAIL (configuration' in refused.stdout
    assert 'real-looking' not in refused.stdout + refused.stderr


@pytest.mark.parametrize('leaf', LEAVES[:2] + LEAVES[-2:], ids=lambda path: path.name)
def test_leaf_checks_refuse_missing_or_non_loopback_simulator(leaf):
    prefix = leaf.parent.name.upper()
    for url in ('', 'http://192.0.2.10:8180', 'http://localhost:8180', 'https://127.0.0.1:8180'):
        values = {f'{prefix}_SIMULATOR_URL': url}
        if prefix == 'RESILIENCE':
            values['RESILIENCE_CONTROL_URL'] = 'http://127.0.0.1:8181'
        refused = run_leaf(leaf, values)
        assert refused.returncode == 1, url
        assert 'FAIL (configuration' in refused.stdout, url


def test_leaf_checks_name_no_external_realm_or_ambient_credentials():
    for leaf in LEAVES:
        text = leaf.read_text()
        assert "'staging'" not in text, leaf.name
        # client.simulator names a realm only for the in-process RawSimulator (no network)
        assert leaf.name == 'client.simulator' or "'production'" not in text, leaf.name
        assert "os.environ.get('B2_APPLICATION_KEY" not in text, leaf.name
        assert "os.environ['B2_APPLICATION_KEY" not in text, leaf.name
        assert 'refusal(' in text, leaf.name


class _JournalServer:
    """A loopback server answering GET /journal with a fixed entry list."""

    def __init__(self, entries):
        import json
        import threading
        from http.server import BaseHTTPRequestHandler, HTTPServer

        body = json.dumps({'entries': entries}).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('content-length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def test_resilience_refuses_a_simulator_that_already_served_requests(capsys):
    contract = load_contract()
    server = _JournalServer([{'seq': 1, 'endpoint': 'b2_upload_file', 'status': 200}])
    try:
        env = environment('resilience', 'upload.cap_exceeded_403')
        env['SDKHARNESS_SIMULATOR_CONTROL_URL'] = server.url
        assert contract.run('resilience', env) == 1
    finally:
        server.close()
    out = capsys.readouterr().out
    assert 'FAIL' in out and 'already served 1 request(s)' in out


def test_fresh_simulator_is_accepted_and_unreadable_journal_is_a_failure():
    contract = load_contract()
    server = _JournalServer([])
    try:
        contract.require_fresh_simulator(server.url)
    finally:
        server.close()
    with pytest.raises(contract.ContractFailure, match='cannot read the simulator journal'):
        contract.require_fresh_simulator('http://127.0.0.1:1')
