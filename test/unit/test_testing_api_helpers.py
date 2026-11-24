######################################################################
#
# File: test/unit/test_testing_api_helpers.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Test live-test setup helpers against the simulator, with HTTP forbidden."""

from __future__ import annotations

import pytest

from b2sdk._internal.api_config import B2HttpApiConfig
from b2sdk._internal.exception import InvalidAuthToken
from b2sdk._internal.raw_simulator import RawSimulator
from b2sdk._internal.testing.helpers.api import authorize, get_b2_auth_data, get_realm


@pytest.fixture
def simulator_config(mocker):
    blocked_http = mocker.patch(
        'requests.sessions.Session.request',
        side_effect=AssertionError('HTTP is forbidden in this test'),
    )
    simulator = RawSimulator()
    config = B2HttpApiConfig(install_clock_skew_hook=False, _raw_api_class=lambda http: simulator)
    yield simulator, config
    blocked_http.assert_not_called()


def test_authorize_populates_returned_in_memory_account_info(simulator_config):
    simulator, config = simulator_config
    key_id, key = simulator.create_account()

    api, info = authorize((key_id, key), 'production', config)

    assert api.account_info is info
    assert api.session.raw_api is simulator
    assert api.api_config is config
    assert info.get_account_id() == key_id
    assert info.get_application_key_id() == key_id
    assert info.get_account_auth_token() == simulator.current_token
    assert info.get_api_url() == simulator.API_URL


def test_authorize_propagates_invalid_credentials_without_creating_token(simulator_config):
    simulator, config = simulator_config
    key_id, _ = simulator.create_account()

    with pytest.raises(InvalidAuthToken, match='secret key is wrong'):
        authorize((key_id, 'incorrect-synthetic-key'), 'production', config)

    assert simulator.auth_token_to_key == {}


@pytest.mark.parametrize('missing_id', [True, False])
def test_missing_environment_credentials_fail_explicitly(monkeypatch, simulator_config, missing_id):
    monkeypatch.delenv('B2_TEST_APPLICATION_KEY_ID', raising=False)
    monkeypatch.delenv('B2_TEST_APPLICATION_KEY', raising=False)
    if not missing_id:
        monkeypatch.setenv('B2_TEST_APPLICATION_KEY_ID', 'synthetic-id')
    missing = 'B2_TEST_APPLICATION_KEY_ID' if missing_id else 'B2_TEST_APPLICATION_KEY'

    with pytest.raises(ValueError, match=f'{missing} is not set'):
        get_b2_auth_data()


def test_environment_values_and_realm_are_explicit(monkeypatch, simulator_config):
    monkeypatch.setenv('B2_TEST_APPLICATION_KEY_ID', 'synthetic-id')
    monkeypatch.setenv('B2_TEST_APPLICATION_KEY', 'synthetic-key')
    monkeypatch.delenv('B2_TEST_ENVIRONMENT', raising=False)

    assert get_b2_auth_data() == ('synthetic-id', 'synthetic-key')
    assert get_realm() == 'production'
    monkeypatch.setenv('B2_TEST_ENVIRONMENT', 'synthetic-realm')
    assert get_realm() == 'synthetic-realm'
