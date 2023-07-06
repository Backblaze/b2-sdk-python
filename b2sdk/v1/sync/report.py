######################################################################
#
# File: b2sdk/v1/sync/report.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import v2


# override to retain legacy methods
class SyncReport(v2.SyncReport):
    @property
    def local_file_count(self):
        return self.total_count

    @local_file_count.setter
    def local_file_count(self, value):
        self.total_count = value

    @property
    def local_done(self):
        return self.total_done

    @local_done.setter
    def local_done(self, value):
        self.total_done = value

    update_local = v2.SyncReport.update_total
    end_local = v2.SyncReport.end_total
