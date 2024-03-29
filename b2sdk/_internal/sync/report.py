######################################################################
#
# File: b2sdk/_internal/sync/report.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import time
import typing
from dataclasses import dataclass

from ..progress import AbstractProgressListener
from ..scan.report import ProgressReport
from ..utils import format_and_scale_fraction, format_and_scale_number

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from io import (
        TextIOWrapper,  # noqa: F401  # sphinx_autodoc_typehints breaks doc build without this import
    )


@dataclass
class SyncReport(ProgressReport):
    """
    Handle reporting progress for syncing.

    Print out each file as it is processed, and puts up a sequence
    of progress bars.

    The progress bars are:
       - Step 1/1: count local files
       - Step 2/2: compare file lists
       - Step 3/3: transfer files

    This class is THREAD SAFE, so it can be used from parallel sync threads.
    """

    def __post_init__(self):
        self.compare_done = False
        self.compare_count = 0
        self.total_transfer_files = 0  # set in end_compare()
        self.total_transfer_bytes = 0  # set in end_compare()
        self.transfer_files = 0
        self.transfer_bytes = 0
        super().__post_init__()

    def _update_progress(self):
        if self.closed or self.no_progress:
            return

        now = time.time()
        interval = now - self._last_update_time
        if interval < self.UPDATE_INTERVAL:
            return

        self._last_update_time = now
        time_delta = now - self.start_time
        rate = 0 if time_delta == 0 else int(self.transfer_bytes / time_delta)
        if not self.total_done:
            message = ' count: %d files   compare: %d files   updated: %d files   %s   %s' % (
                self.total_count,
                self.compare_count,
                self.transfer_files,
                format_and_scale_number(self.transfer_bytes, 'B'),
                format_and_scale_number(rate, 'B/s')
            )  # yapf: disable
        elif not self.compare_done:
            message = ' compare: %d/%d files   updated: %d files   %s   %s' % (
                self.compare_count,
                self.total_count,
                self.transfer_files,
                format_and_scale_number(self.transfer_bytes, 'B'),
                format_and_scale_number(rate, 'B/s')
            )  # yapf: disable
        else:
            message = ' compare: %d/%d files   updated: %d/%d files   %s   %s' % (
                self.compare_count,
                self.total_count,
                self.transfer_files,
                self.total_transfer_files,
                format_and_scale_fraction(self.transfer_bytes, self.total_transfer_bytes, 'B'),
                format_and_scale_number(rate, 'B/s')
            )  # yapf: disable
        self._print_line(message, False)

    def update_compare(self, delta):
        """
        Report that more files have been compared.

        :param delta: number of files compared
        :type delta: int
        """
        with self.lock:
            self.compare_count += delta
            self._update_progress()

    def end_compare(self, total_transfer_files, total_transfer_bytes):
        """
        Report that the comparison has been finished.

        :param total_transfer_files: total number of transferred files
        :type total_transfer_files: int
        :param total_transfer_bytes: total number of transferred bytes
        :type total_transfer_bytes: int
        """
        with self.lock:
            self.compare_done = True
            self.total_transfer_files = total_transfer_files
            self.total_transfer_bytes = total_transfer_bytes
            self._update_progress()

    def update_transfer(self, file_delta, byte_delta):
        """
        Update transfer info.

        :param file_delta: number of files transferred
        :type file_delta: int
        :param byte_delta: number of bytes transferred
        :type byte_delta: int
        """
        with self.lock:
            self.transfer_files += file_delta
            self.transfer_bytes += byte_delta
            self._update_progress()


class SyncFileReporter(AbstractProgressListener):
    """
    Listen to the progress for a single file and pass info on to a SyncReporter.
    """

    def __init__(self, reporter, *args, **kwargs):
        """
        :param reporter: a reporter object
        """
        super().__init__(*args, **kwargs)
        self.bytes_so_far = 0
        self.reporter = reporter

    def close(self):
        """
        Perform a clean-up.
        """
        # no more bytes are done, but the file is done
        self.reporter.update_transfer(1, 0)

    def set_total_bytes(self, total_byte_count):
        """
        Set total bytes count.

        :param total_byte_count: total byte count
        :type total_byte_count: int
        """
        pass

    def bytes_completed(self, byte_count):
        """
        Set bytes completed count.

        :param byte_count: total byte count
        :type byte_count: int
        """
        self.reporter.update_transfer(0, byte_count - self.bytes_so_far)
        self.bytes_so_far = byte_count


def sample_sync_report_run():
    """
    Generate a sample report.
    """
    import sys
    sync_report = SyncReport(sys.stdout, False)

    for i in range(20):
        sync_report.update_total(1)
        time.sleep(0.2)
        if i == 10:
            sync_report.print_completion('transferred: a.txt')
        if i % 2 == 0:
            sync_report.update_compare(1)
    sync_report.end_total()

    for i in range(10):
        sync_report.update_compare(1)
        time.sleep(0.2)
        if i == 3:
            sync_report.print_completion('transferred: b.txt')
        if i == 4:
            sync_report.update_transfer(25, 25000)
    sync_report.end_compare(50, 50000)

    for i in range(25):
        if i % 2 == 0:
            sync_report.print_completion('transferred: %d.txt' % i)
        sync_report.update_transfer(1, 1000)
        time.sleep(0.2)

    sync_report.close()
