######################################################################
#
# File: test/unit/fixtures/session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import B2Session


@pytest.fixture
def b2_session(fake_account_info, fake_cache, fake_b2_raw_api):
    session = B2Session(account_info=fake_account_info, cache=fake_cache)
    session.raw_api = fake_b2_raw_api
    return session


@pytest.fixture
def fake_b2_session(mocker, fake_account_info, fake_cache, fake_b2_raw_api):
    b2_session = mocker.MagicMock(name='FakeB2Session', spec=B2Session)
    b2_session.account_info = fake_account_info
    b2_session.cache = fake_cache
    b2_session.raw_api = fake_b2_raw_api
    return b2_session
