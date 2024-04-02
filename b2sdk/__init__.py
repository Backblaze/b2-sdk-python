######################################################################
#
# File: b2sdk/__init__.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import b2sdk.version  # noqa: E402
__version__ = b2sdk.version.VERSION
assert __version__  # PEP-0396
