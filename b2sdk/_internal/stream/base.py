######################################################################
#
# File: b2sdk/_internal/stream/base.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io


class ReadOnlyStreamMixin:
    def writeable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('Cannot accept a write to a read-only stream')
