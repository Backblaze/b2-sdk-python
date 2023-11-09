######################################################################
#
# File: test/unit/v2/test_utils.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import os.path

import pytest

from .apiver.apiver_deps import TempDir


def test_temp_dir() -> None:
    with pytest.deprecated_call():
        with TempDir() as temp_dir:
            assert os.path.exists(temp_dir)
    assert not os.path.exists(temp_dir)
