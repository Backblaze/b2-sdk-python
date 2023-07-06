######################################################################
#
# File: b2sdk/v1/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk.v2.exception import *  # noqa
v2DestFileNewer = DestFileNewer


# This exception class is deprecated and should not be used in new designs
class CommandError(B2Error):
    """
    b2 command error (user caused).  Accepts exactly one argument: message.

    We expect users of shell scripts will parse our ``__str__`` output.
    """

    def __init__(self, message):
        super().__init__()
        self.message = message

    def __str__(self):
        return self.message


class DestFileNewer(v2DestFileNewer):
    def __init__(self, dest_file, source_file, dest_prefix, source_prefix):
        super(v2DestFileNewer, self).__init__()
        self.dest_file = dest_file
        self.source_file = source_file
        self.dest_prefix = dest_prefix
        self.source_prefix = source_prefix

    def __str__(self):
        return 'source file is older than destination: {}{} with a time of {} cannot be synced to {}{} with a time of {}, unless a valid newer_file_mode is provided'.format(
            self.source_prefix,
            self.source_file.name,
            self.source_file.latest_version().mod_time,
            self.dest_prefix,
            self.dest_file.name,
            self.dest_file.latest_version().mod_time,
        )
