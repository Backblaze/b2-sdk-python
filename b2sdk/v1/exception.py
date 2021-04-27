######################################################################
#
# File: b2sdk/v1/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk._v2.exception import *  # noqa


# This exception class is deprecated and should not be used in new designs
class CommandError(B2Error):
    """
    b2 command error (user caused).  Accepts exactly one argument: message.

    We expect users of shell scripts will parse our ``__str__`` output.
    """

    def __init__(self, message):
        super(CommandError, self).__init__()
        self.message = message

    def __str__(self):
        return self.message
