######################################################################
#
# File: b2sdk/v2/session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import _v3 as v3
from .b2http import B2Http

from ._compat import _file_infos_rename


# Override to use legacy B2Http
class B2Session(v3.B2Session):
    B2HTTP_CLASS = staticmethod(B2Http)

    @_file_infos_rename
    def upload_file(self, *args, **kwargs):
        return super().upload_file(*args, **kwargs)
