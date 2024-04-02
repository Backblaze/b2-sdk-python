######################################################################
#
# File: b2sdk/_internal/sync/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from ..exception import B2SimpleError


class IncompleteSync(B2SimpleError):
    pass
