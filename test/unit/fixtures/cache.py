######################################################################
#
# File: test/unit/fixtures/cache.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import InMemoryCache


@pytest.fixture
def fake_cache(mocker):
    return mocker.MagicMock(name='FakeCache', spec=InMemoryCache)
