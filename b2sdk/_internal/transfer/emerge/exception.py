######################################################################
#
# File: b2sdk/_internal/transfer/emerge/exception.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal.exception import B2SimpleError


class UnboundStreamBufferTimeout(B2SimpleError):
    """
    Raised when there is no space for a new buffer for a certain amount of time.
    """
    pass
