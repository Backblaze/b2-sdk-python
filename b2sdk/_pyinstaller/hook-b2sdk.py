######################################################################
#
# File: b2sdk/_pyinstaller/hook-b2sdk.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata('b2sdk')
