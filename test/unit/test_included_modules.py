######################################################################
#
# File: test/unit/test_included_modules.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pathlib

from b2sdk import requests
from b2sdk.requests.included_source_meta import included_source_meta


def test_requests_notice_file():
    with (pathlib.Path(requests.__file__).parent / 'NOTICE').open('r') as notice_file:
        assert notice_file.read() == included_source_meta.files['NOTICE']
