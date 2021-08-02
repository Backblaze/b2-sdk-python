######################################################################
#
# File: b2sdk/v2/session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v3 as v3
from .b2http import B2Http


# Override to use legacy B2Http
class B2Session(v3.B2Session):
    B2HTTP_CLASS = staticmethod(B2Http)
