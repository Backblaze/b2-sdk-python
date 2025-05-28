######################################################################
#
# File: test/unit/fixtures/raw_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from copy import copy

import pytest
from apiver_deps import ALL_CAPABILITIES, B2RawHTTPApi


@pytest.fixture
def fake_b2_raw_api_responses(apiver_int):
    capabilities = copy(ALL_CAPABILITIES)
    namePrefix = None

    storage_api = {
        'downloadUrl': 'https://f000.backblazeb2.xyz:8180',
        'absoluteMinimumPartSize': 5000000,
        'recommendedPartSize': 100000000,
        'apiUrl': 'https://api000.backblazeb2.xyz:8180',
        's3ApiUrl': 'https://s3.us-west-000.backblazeb2.xyz:8180',
    }

    if apiver_int < 3:
        storage_api.update(
            {
                'capabilities': capabilities,
                'namePrefix': namePrefix,
                'bucketId': None,
                'bucketName': None,
            }
        )
    else:
        storage_api['allowed'] = {
            'buckets': None,
            'capabilities': capabilities,
            'namePrefix': namePrefix,
        }

    return {
        'authorize_account': {
            'accountId': '6012deadbeef',
            'apiInfo': {
                'groupsApi': {},
                'storageApi': storage_api,
            },
            'authorizationToken': '4_1111111111111111111111111_11111111_111111_1111_1111111111111_1111_11111111=',
        }
    }


@pytest.fixture
def fake_b2_raw_api(mocker, fake_b2http, fake_b2_raw_api_responses):
    raw_api = mocker.MagicMock(name='FakeB2RawHTTPApi', spec=B2RawHTTPApi)
    raw_api.b2_http = fake_b2http
    raw_api.authorize_account.return_value = fake_b2_raw_api_responses['authorize_account']
    return raw_api
