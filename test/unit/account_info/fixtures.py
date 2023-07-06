######################################################################
#
# File: test/unit/account_info/fixtures.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from apiver_deps import InMemoryAccountInfo, SqliteAccountInfo


@pytest.fixture
def account_info_default_data_schema_0():
    return dict(
        account_id='account_id',
        auth_token='account_auth',
        api_url='https://api000.backblazeb2.xyz:8180',
        download_url='https://f000.backblazeb2.xyz:8180',
        minimum_part_size=100,
        application_key='app_key',
        realm='dev',
    )


@pytest.fixture
def account_info_default_data(account_info_default_data_schema_0, apiver):
    if apiver in ['v0', 'v1']:
        return dict(
            allowed=None,
            application_key_id='application_key_id',
            s3_api_url='https://s3.us-west-000.backblazeb2.xyz:8180',
            **account_info_default_data_schema_0,
        )
    return dict(
        allowed=None,
        application_key_id='application_key_id',
        s3_api_url='https://s3.us-west-000.backblazeb2.xyz:8180',
        account_id='account_id',
        auth_token='account_auth',
        api_url='https://api000.backblazeb2.xyz:8180',
        download_url='https://f000.backblazeb2.xyz:8180',
        recommended_part_size=100,
        absolute_minimum_part_size=50,
        application_key='app_key',
        realm='dev',
    )


@pytest.fixture(scope='session')
def in_memory_account_info_factory():
    def get_account_info():
        return InMemoryAccountInfo()

    return get_account_info


@pytest.fixture
def in_memory_account_info(in_memory_account_info_factory):
    return in_memory_account_info_factory()


@pytest.fixture
def sqlite_account_info_factory(tmpdir):
    def get_account_info(file_name=None, schema_0=False):
        if file_name is None:
            file_name = str(tmpdir.join('b2_account_info'))
        if schema_0:
            last_upgrade_to_run = 0
        else:
            last_upgrade_to_run = None
        return SqliteAccountInfo(file_name, last_upgrade_to_run)

    return get_account_info


@pytest.fixture
def sqlite_account_info(sqlite_account_info_factory):
    return sqlite_account_info_factory()


@pytest.fixture(
    params=[
        pytest.lazy_fixture('in_memory_account_info_factory'),
        pytest.lazy_fixture('sqlite_account_info_factory'),
    ]
)
def account_info_factory(request):
    return request.param


@pytest.fixture(
    params=[
        pytest.lazy_fixture('in_memory_account_info'),
        pytest.lazy_fixture('sqlite_account_info'),
    ]
)
def account_info(request):
    return request.param


@pytest.fixture
def fake_account_info(mocker):
    account_info = mocker.MagicMock(name='FakeAccountInfo', spec=InMemoryAccountInfo)
    account_info.REALM_URLS = {
        'dev': 'http://api.backblazeb2.xyz:8180',
    }
    account_info.is_same_account.return_value = True
    account_info.is_same_key.return_value = True
    return account_info
