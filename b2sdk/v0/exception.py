######################################################################
#
# File: b2sdk/v0/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

B2Error = None  # calm down, pyflakes

from b2sdk.v1.exception import *  # noqa

v1DestFileNewer = DestFileNewer


# override to retain old style __str__
class DestFileNewer(v1DestFileNewer):
    def __str__(self):
        return 'source file is older than destination: %s%s with a time of %s cannot be synced to %s%s with a time of %s, unless --skipNewer or --replaceNewer is provided' % (
            self.source_prefix,
            self.source_file.name,
            self.source_file.latest_version().mod_time,
            self.dest_prefix,
            self.dest_file.name,
            self.dest_file.latest_version().mod_time,
        )
