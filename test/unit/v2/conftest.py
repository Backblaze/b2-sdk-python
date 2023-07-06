######################################################################
#
# File: test/unit/v2/conftest.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest


@pytest.fixture
def file_info():
    return {'key': 'value'}
