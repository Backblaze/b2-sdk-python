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
def fake_b2_raw_api_responses():
    return {
        'authorize_account': {
            'absoluteMinimumPartSize': 5000000,
            'accountId': '6012deadbeef',
            'allowed': {
                'bucketId': None,
                'bucketName': None,
                'capabilities': copy(ALL_CAPABILITIES),
                'namePrefix': None,
            },
            'apiUrl': 'https://api000.backblazeb2.xyz:8180',
            'authorizationToken': '4_1111111111111111111111111_11111111_111111_1111_1111111111111_1111_11111111=',
            'downloadUrl': 'https://f000.backblazeb2.xyz:8180',
            'recommendedPartSize': 100000000,
            's3ApiUrl': 'https://s3.us-west-000.backblazeb2.xyz:8180',
        }
    }  # yapf: disable


@pytest.fixture
def fake_b2_raw_api(mocker, fake_b2http, fake_b2_raw_api_responses):
    raw_api = mocker.MagicMock(name='FakeB2RawHTTPApi', spec=B2RawHTTPApi)
    raw_api.b2_http = fake_b2http
    raw_api.authorize_account.return_value = fake_b2_raw_api_responses['authorize_account']
    return raw_api
