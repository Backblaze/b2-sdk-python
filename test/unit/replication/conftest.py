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
    Bucket,
    ReplicationConfiguration,
    ReplicationMonitor,
    ReplicationRule,
)


@pytest.fixture
def destination_bucket(b2api) -> Bucket:
    return b2api.create_bucket('destination-bucket', 'allPublic')


@pytest.fixture
def source_bucket(b2api, destination_bucket) -> Bucket:
    bucket = b2api.create_bucket('source-bucket', 'allPublic')

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
