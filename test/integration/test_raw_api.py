######################################################################
#
# File: test/integration/test_raw_api.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from b2sdk import raw_api


# TODO: move the test_raw_api test logic here
def test_raw_api():
    if raw_api.test_raw_api():
        pytest.fail('test_raw_api exited with non-zero exit code')
