######################################################################
#
# File: b2sdk/sync/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..exception import B2SimpleError
from ..scan.exception import BaseDirectoryError


class IncompleteSync(B2SimpleError):
    pass
