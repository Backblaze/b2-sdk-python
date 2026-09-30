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
