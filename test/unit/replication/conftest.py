######################################################################
#
# File: test/unit/replication/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from apiver_deps import (
    B2Api,
    B2HttpApiConfig,
    Bucket,
    RawSimulator,
    ReplicationConfiguration,
    ReplicationMonitor,
    ReplicationRule,
    StubAccountInfo,
)


@pytest.fixture
def api() -> B2Api:
    account_info = StubAccountInfo()
    api = B2Api(
        account_info,
        api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
    )

    simulator = api.session.raw_api
    account_id, master_key = simulator.create_account()
    api.authorize_account(
        application_key_id=account_id,
        application_key=master_key,
        realm='production',
    )
    # api_url = account_info.get_api_url()
    # account_auth_token = account_info.get_account_auth_token()1
    return api


@pytest.fixture
def destination_bucket(api) -> Bucket:
    return api.create_bucket('destination-bucket', 'allPublic')


@pytest.fixture
def source_bucket(api, destination_bucket) -> Bucket:
    bucket = api.create_bucket('source-bucket', 'allPublic')

    bucket.replication = ReplicationConfiguration(
        rules=[
            ReplicationRule(
                destination_bucket_id=destination_bucket.id_,
                name='name',
                file_name_prefix='folder/',
            ),
        ],
        source_key_id='hoho|trololo',
    )

    return bucket


@pytest.fixture
def test_file(tmpdir) -> str:
    file = tmpdir.join('test.txt')
    file.write('whatever')
    return file


@pytest.fixture
def test_file_reversed(tmpdir) -> str:
    file = tmpdir.join('test-reversed.txt')
    file.write('revetahw')
    return file


@pytest.fixture
def monitor(source_bucket) -> ReplicationMonitor:
    return ReplicationMonitor(
        source_bucket,
        rule=source_bucket.replication.rules[0],
    )
