######################################################################
#
# File: b2sdk/_internal/transfer/outbound/large_file_upload_state.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import threading


class LargeFileUploadState:
    """
    Track the status of uploading a large file, accepting updates
    from the tasks that upload each of the parts.

    The aggregated progress is passed on to a ProgressListener that
    reports the progress for the file as a whole.

    This class is THREAD SAFE.
    """

    def __init__(self, file_progress_listener):
        """
        :param b2sdk.v2.AbstractProgressListener file_progress_listener: a progress listener object to use. Use :py:class:`b2sdk.v2.DoNothingProgressListener` to disable.
        """
        self.lock = threading.RLock()
        self.error_message = None
        self.file_progress_listener = file_progress_listener
        self.part_number_to_part_state = {}
        self.bytes_completed = 0

    def set_error(self, message):
        """
        Set an error message.

        :param str message: an error message
        """
        with self.lock:
            self.error_message = message

    def has_error(self):
        """
        Check whether an error occurred.

        :rtype: bool
        """
        with self.lock:
            return self.error_message is not None

    def get_error_message(self):
        """
        Fetche an error message.

        :return: an error message
        :rtype: str
        """
        with self.lock:
            assert self.has_error()
            return self.error_message

    def update_part_bytes(self, bytes_delta):
        """
        Update listener progress info.

        :param int bytes_delta: number of bytes to increase a progress for
        """
        with self.lock:
            self.bytes_completed += bytes_delta
            self.file_progress_listener.bytes_completed(self.bytes_completed)
