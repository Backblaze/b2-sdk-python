######################################################################
#
# File: test/unit/account_info/fixtures.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import InMemoryAccountInfo, SqliteAccountInfo


@pytest.fixture(scope='session')
def in_memory_account_info_factory():
    def get_account_info(
        account_id='account_id',
        auth_token='account_auth',
        api_url='https://api000.backblazeb2.com',
        download_url='download_url',
        minimum_part_size=100,
        application_key='app_key',
        realm='realm',
        allowed=None,
        application_key_id=None,
        s3_api_url=None
    ):
        account_info = InMemoryAccountInfo()
        account_info.set_auth_data(
            account_id=account_id,
            auth_token=auth_token,
            api_url=api_url,
            download_url=download_url,
            minimum_part_size=minimum_part_size,
            application_key=application_key,
            realm=realm,
            allowed=allowed,
            application_key_id=application_key_id,
            s3_api_url=s3_api_url,
        )
        return account_info

    return get_account_info


@pytest.fixture
def in_memory_account_info(in_memory_account_info_factory):
    return in_memory_account_info_factory()


@pytest.fixture
def sqlite_account_info_factory(tmpdir):
    def get_account_info(
        account_id='account_id',
        auth_token='account_auth',
        api_url='https://api000.backblazeb2.com',
        download_url='download_url',
        minimum_part_size=100,
        application_key='app_key',
        realm='realm',
        allowed=None,
        application_key_id=None,
        s3_api_url=None,
        *,
        file_name=None,
        schema_0=False
    ):
        if file_name is None:
            file_name = str(tmpdir.join('b2_account_info'))

        if schema_0:
            last_upgrade_to_run = 0
        else:
            last_upgrade_to_run = None

        account_info = SqliteAccountInfo(file_name, last_upgrade_to_run)
        if schema_0:
            account_info.set_auth_data_with_schema_0_for_test(
                account_id=account_id,
                auth_token=auth_token,
                api_url=api_url,
                download_url=download_url,
                minimum_part_size=minimum_part_size,
                application_key=application_key,
                realm=realm,
            )
        else:
            account_info.set_auth_data(
                account_id=account_id,
                auth_token=auth_token,
                api_url=api_url,
                download_url=download_url,
                minimum_part_size=minimum_part_size,
                application_key=application_key,
                realm=realm,
                allowed=allowed,
                application_key_id=application_key_id,
                s3_api_url=s3_api_url,
            )
        return account_info

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
