######################################################################
#
# File: b2sdk/_internal/testing/fixtures/common.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest


@pytest.fixture(scope='session')
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp
