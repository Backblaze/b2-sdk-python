######################################################################
#
# File: test/conftest.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import concurrent.futures

import pytest


@pytest.fixture
def bg_executor():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        yield executor
