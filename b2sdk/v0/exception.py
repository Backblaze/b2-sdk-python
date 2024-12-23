######################################################################
#
# File: b2sdk/v0/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

B2Error = None  # calm down, pyflakes

from b2sdk.v1.exception import *  # noqa

v1DestFileNewer = DestFileNewer


# override to retain old style __str__
class DestFileNewer(v1DestFileNewer):
    def __str__(self):
        return f'source file is older than destination: {self.source_prefix}{self.source_file.name} with a time of {self.source_file.latest_version().mod_time} cannot be synced to {self.dest_prefix}{self.dest_file.name} with a time of {self.dest_file.latest_version().mod_time}, unless --skipNewer or --replaceNewer is provided'
