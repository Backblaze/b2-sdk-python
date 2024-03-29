######################################################################
#
# File: b2sdk/_internal/transfer/outbound/progress_reporter.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal.progress import AbstractProgressListener


class PartProgressReporter(AbstractProgressListener):
    """
    An adapter that listens to the progress of upload a part and
    gives the information to a :py:class:`b2sdk._internal.transfer.outbound.large_file_upload_state.LargeFileUploadState`.

    Accepts absolute bytes_completed from the uploader, and reports
    deltas to the :py:class:`b2sdk._internal.transfer.outbound.large_file_upload_state.LargeFileUploadState`.  The bytes_completed for the
    part will drop back to 0 on a retry, which will result in a
    negative delta.
    """

    def __init__(self, large_file_upload_state, *args, **kwargs):
        """
        :param b2sdk._internal.transfer.outbound.large_file_upload_state.LargeFileUploadState large_file_upload_state: object to relay the progress to
        """
        super().__init__(*args, **kwargs)
        self.large_file_upload_state = large_file_upload_state
        self.prev_byte_count = 0

    def bytes_completed(self, byte_count):
        self.large_file_upload_state.update_part_bytes(byte_count - self.prev_byte_count)
        self.prev_byte_count = byte_count

    def close(self):
        pass

    def set_total_bytes(self, total_byte_count):
        pass
