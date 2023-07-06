######################################################################
#
# File: test/static/test_licenses.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from glob import glob
from itertools import islice

import pytest


def test_files_headers():
    for file in glob('**/*.py', recursive=True):
        with open(file) as fd:
            file = file.replace(
                '\\', '/'
            )  # glob('**/*.py') on Windows returns "b2\bucket.py" (wrong slash)
            head = ''.join(islice(fd, 9))
            if 'All Rights Reserved' not in head:
                pytest.fail(f'Missing "All Rights Reserved" in the header in: {file}')
            if file not in head:
                pytest.fail(f'Wrong file name in the header in: {file}')
