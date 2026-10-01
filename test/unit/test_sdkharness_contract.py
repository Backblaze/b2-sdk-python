######################################################################
#
# File: test/unit/test_sdkharness_contract.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / '.sdkharness' / 'tests' / 'health-golden-path'


def load_contract():
    loader = SourceFileLoader('sdkharness_health_golden_path', str(SCRIPT))
    spec = spec_from_loader(loader.name, loader)
    module = module_from_spec(spec)
    loader.exec_module(module)
    return module


def valid_environment():
    return {
        'SDKHARNESS_TEST_LEVEL': 'health',
        'SDKHARNESS_SCENARIO': 'golden-path',
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8180',
        'HEALTHCHECK_REALM_URL': 'http://127.0.0.1:8180',
        'B2_TEST_APPLICATION_KEY_ID': 'test-key-id',
        'B2_TEST_APPLICATION_KEY': 'test-key',
        'B2_BUCKET_NAME': 'sdkharness-healthcheck',
    }


class FileVersion:
    def __init__(self, file_name):
        self.file_name = file_name
        self.id_ = 'file-id'


class Download:
    def __init__(self, payload):
        self.payload = payload

    def save(self, destination):
        destination.write(self.payload)


class Bucket:
    def __init__(self):
        self.files = {}
        self.deleted = []

    def upload_bytes(self, payload, name):
        self.files[name] = payload
        return FileVersion(name)

    def download_file_by_name(self, name):
        return Download(self.files[name])

    def ls(self, name, recursive=True):
        del recursive
        return [(FileVersion(name), None)] if name in self.files else []

    def delete_file_version(self, file_id, name):
        self.deleted.append((file_id, name))
        del self.files[name]


class Api:
    def __init__(self, bucket):
        self.bucket = bucket
        self.authorization = None

    def authorize_account(self, key_id, application_key, realm):
        self.authorization = (key_id, application_key, realm)

    def get_bucket_by_name(self, name):
        assert name == 'sdkharness-healthcheck'
        return self.bucket


def test_customer_health_lifecycle():
    contract = load_contract()
    bucket = Bucket()
    api = Api(bucket)
    contract.run_health(
        valid_environment(), api_factory=lambda: api, object_name='health/object.txt'
    )

    assert api.authorization == ('test-key-id', 'test-key', 'http://127.0.0.1:8180')
    assert bucket.files == {}
    assert bucket.deleted == [('file-id', 'health/object.txt')]


@pytest.mark.parametrize(
    'url',
    [
        'https://api.backblazeb2.com/',
        'http://localhost:8180/',
        'http://127.0.0.1/',
        'http://user@127.0.0.1:8180/',
        'http://127.0.0.1:8180/not-root',
        'http://[::1]:8180/',
    ],
)
def test_rejects_non_loopback_simulator_contract(url):
    contract = load_contract()
    environment = valid_environment()
    environment['SDKHARNESS_SIMULATOR_URL'] = url
    environment['HEALTHCHECK_REALM_URL'] = url

    with pytest.raises(contract.CheckFailure, match='configuration'):
        contract.validate_environment(environment)


def test_failure_result_is_one_credential_safe_record(monkeypatch, capsys):
    contract = load_contract()
    for name, value in valid_environment().items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv('B2_BUCKET_NAME')

    assert contract.main() == 1
    output = capsys.readouterr().out
    assert output == (
        'SDKHARNESS_RESULT\thealth\tgolden-path\tFAIL\t'
        'configuration: required simulator input is missing\n'
    )
    assert 'test-key' not in output


@pytest.mark.parametrize(
    'key_id, key',
    [
        ('005realkeyid0000000000000', 'K005realapplicationkey00000000000'),
        ('test-key-id', 'K005realapplicationkey00000000000'),
        ('005realkeyid0000000000000', 'test-key'),
    ],
)
def test_ambient_real_looking_credentials_never_reach_the_sdk(key_id, key):
    contract = load_contract()
    environment = valid_environment()
    environment['B2_TEST_APPLICATION_KEY_ID'] = key_id
    environment['B2_TEST_APPLICATION_KEY'] = key

    class ForbiddenApi:
        def authorize_account(self, *args, **kwargs):  # pragma: no cover - must not run
            raise AssertionError('a non-simulator credential reached the SDK')

    with pytest.raises(contract.CheckFailure) as raised:
        contract.run_health(environment, api_factory=lambda: ForbiddenApi())
    assert raised.value.step == 'configuration'
    assert raised.value.detail == 'only the fixed simulator credential is accepted'
    assert 'K005' not in str(raised.value.detail)
    assert '005real' not in str(raised.value.detail)
