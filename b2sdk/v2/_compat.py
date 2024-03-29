######################################################################
#
# File: b2sdk/v2/_compat.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
from b2sdk._internal import version_utils

_file_infos_rename = version_utils.rename_argument('file_infos', 'file_info', None, 'v3')
