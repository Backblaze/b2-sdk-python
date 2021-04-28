######################################################################
#
# File: test/unit/fixtures/b2http.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import B2Http


@pytest.fixture
def fake_b2http(mocker):
    return mocker.MagicMock(name='FakeB2Http', spec=B2Http)
