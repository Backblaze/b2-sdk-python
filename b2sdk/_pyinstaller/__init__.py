######################################################################
#
# File: b2sdk/_pyinstaller/__init__.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os


def get_hook_dirs():
    """Get hooks directories for pyinstaller.

    More info about the hooks:
    https://pyinstaller.readthedocs.io/en/stable/hooks.html
    """
    return [os.path.dirname(__file__)]
