######################################################################
#
# File: test/integration/test_raw_api.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os

import pytest

from b2sdk import raw_api


# TODO: move the test_raw_api test logic here
def test_raw_api():
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        pytest.fail('B2_TEST_APPLICATION_KEY_ID is not set.')

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        pytest.fail('B2_TEST_APPLICATION_KEY is not set.')

    print()

    if raw_api.test_raw_api():
        pytest.fail('test_raw_api exited with non-zero exit code')
