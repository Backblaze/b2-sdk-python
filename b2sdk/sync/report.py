######################################################################
#
# File: b2sdk/sync/report.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import threading
import time

import six

from ..progress import AbstractProgressListener
from ..utils import format_and_scale_number, format_and_scale_fraction

logger = logging.getLogger(__name__)


class SyncReport(object):
    """
    Handles reporting progress for syncing.

    Prints out each file as it is processed, and puts up a sequence
    of progress bars.

    The progress bars are:
       - Step 1/1: count local files
       - Step 2/2: compare file lists
       - Step 3/3: transfer files

    This class is THREAD SAFE so that it can be used from parallel sync threads.
    """

    # Minimum time between displayed updates
    UPDATE_INTERVAL = 0.1

    def __init__(self, stdout, no_progress):
        """
        :param stdout: standard output file object
        :param no_progress: if True, do not show progress
        :type no_progress: bool
        """
        self.stdout = stdout
        self.no_progress = no_progress
        self.start_time = time.time()
        self.local_file_count = 0
        self.local_done = False
        self.compare_done = False
        self.compare_count = 0
        self.total_transfer_files = 0  # set in end_compare()
        self.total_transfer_bytes = 0  # set in end_compare()
        self.transfer_files = 0
        self.transfer_bytes = 0
        self.current_line = ''
        self._last_update_time = 0
        self.closed = False
        self.lock = threading.Lock()
        self.encoding_warning_was_already_printed = False
        self._update_progress()
        self.warnings = []

    def close(self):
        """
        Perform clean up
        """
        with self.lock:
            if not self.no_progress:
                self._print_line('', False)
            self.closed = True
            for warning in self.warnings:
                self._print_line(warning, True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def error(self, message):
        """
        Print an error, gracefully interleaving it with a progress bar

        :param message: an error message
        :type message: str
        """
        self.print_completion(message)

    def print_completion(self, message):
        """
        Removes the progress bar, prints a message, and puts the progress
        bar back.

        :param message: an error message
        :type message: str
        """
        with self.lock:
            if not self.closed:
                self._print_line(message, True)
                self._last_update_time = 0
                self._update_progress()

    def _update_progress(self):
        if not self.closed and not self.no_progress:
            now = time.time()
            interval = now - self._last_update_time
            if self.UPDATE_INTERVAL <= interval:
                self._last_update_time = now
                time_delta = time.time() - self.start_time
                rate = 0 if time_delta == 0 else int(self.transfer_bytes / time_delta)
                if not self.local_done:
                    message = ' count: %d files   compare: %d files   updated: %d files   %s   %s' % (
                        self.local_file_count,
                        self.compare_count,
                        self.transfer_files,
                        format_and_scale_number(self.transfer_bytes, 'B'),
                        format_and_scale_number(rate, 'B/s')
                    )  # yapf: disable
                elif not self.compare_done:
                    message = ' compare: %d/%d files   updated: %d files   %s   %s' % (
                        self.compare_count,
                        self.local_file_count,
                        self.transfer_files,
                        format_and_scale_number(self.transfer_bytes, 'B'),
                        format_and_scale_number(rate, 'B/s')
                    )  # yapf: disable
                else:
                    message = ' compare: %d/%d files   updated: %d/%d files   %s   %s' % (
                        self.compare_count,
                        self.local_file_count,
                        self.transfer_files,
                        self.total_transfer_files,
                        format_and_scale_fraction(self.transfer_bytes, self.total_transfer_bytes, 'B'),
                        format_and_scale_number(rate, 'B/s')
                    )  # yapf: disable
                self._print_line(message, False)

    def _print_line(self, line, newline):
        """
        Prints a line to stdout.

        :param line: A string without a \r or \n in it.
        :type line: str
        :param newline: True if the output should move to a new line after this one.
        :type newline: bool
        """
        if len(line) < len(self.current_line):
            line += ' ' * (len(self.current_line) - len(line))
        try:
            self.stdout.write(line)
        except UnicodeEncodeError as encode_error:
            if not self.encoding_warning_was_already_printed:
                self.encoding_warning_was_already_printed = True
                self.stdout.write(
                    '!WARNING! this terminal cannot properly handle progress reporting.  encoding is %s.\n'
                    % (self.stdout.encoding,)
                )
            self.stdout.write(line.encode('ascii', 'backslashreplace').decode())
            logger.warning(
                'could not output the following line with encoding %s on stdout due to %s: %s' %
                (self.stdout.encoding, encode_error, line)
            )
        if newline:
            self.stdout.write('\n')
            self.current_line = ''
        else:
            self.stdout.write('\r')
            self.current_line = line
        self.stdout.flush()

    def update_local(self, delta):
        """
        Reports that more local files have been found.

        :param delta: number of files found since the last check
        :type delta: int
        """
        with self.lock:
            self.local_file_count += delta
            self._update_progress()

    def end_local(self):
        """
        Local file count is done.  Can proceed to step 2.
        """
        with self.lock:
            self.local_done = True
            self._update_progress()

    def update_compare(self, delta):
        """
        Reports that more files have been compared.

        :param delta: number of files compared
        :type delta: int
        """
        with self.lock:
            self.compare_count += delta
            self._update_progress()

    def end_compare(self, total_transfer_files, total_transfer_bytes):
        """
        Report that the comparison has been finished

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
        Update transfer info

        :param file_delta: number of files transferred
        :type file_delta: int
        :param byte_delta: number of bytes transferred
        :type byte_delta: int
        """
        with self.lock:
            self.transfer_files += file_delta
            self.transfer_bytes += byte_delta
            self._update_progress()

    def local_access_error(self, path):
        """
        Add a file access error message to the list of warnings

        :param path: file path
        :type path: str
        """
        self.warnings.append('WARNING: %s could not be accessed (broken symlink?)' % (path,))

    def local_permission_error(self, path):
        """
        Add a permission error message to the list of warnings

        :param path: file path
        :type path: str
        """
        self.warnings.append(
            'WARNING: %s could not be accessed (no permissions to read?)' % (path,)
        )

    def symlink_skipped(self, path):
        pass


class SyncFileReporter(AbstractProgressListener):
    """
    Listens to the progress for a single file and passes info on to a SyncReporter.
    """

    def __init__(self, reporter, *args, **kwargs):
        """
        :param reporter: a reporter object
        """
        super(SyncFileReporter, self).__init__(*args, **kwargs)
        self.bytes_so_far = 0
        self.reporter = reporter

    def close(self):
        """
        Perform clean up
        """
        # no more bytes are done, but the file is done
        self.reporter.update_transfer(1, 0)

    def set_total_bytes(self, total_byte_count):
        """
        Set total bytes count

        :param total_byte_count: total byte count
        :type total_byte_count: int
        """
        pass

    def bytes_completed(self, byte_count):
        """
        Set bytes completed count

        :param byte_count: total byte count
        :type byte_count: int
        """
        self.reporter.update_transfer(0, byte_count - self.bytes_so_far)
        self.bytes_so_far = byte_count


def sample_sync_report_run():
    """
    Generate sample report
    """
    import sys
    sync_report = SyncReport(sys.stdout, False)

    for i in six.moves.range(20):
        sync_report.update_local(1)
        time.sleep(0.2)
        if i == 10:
            sync_report.print_completion('transferred: a.txt')
        if i % 2 == 0:
            sync_report.update_compare(1)
    sync_report.end_local()

    for i in six.moves.range(10):
        sync_report.update_compare(1)
        time.sleep(0.2)
        if i == 3:
            sync_report.print_completion('transferred: b.txt')
        if i == 4:
            sync_report.update_transfer(25, 25000)
    sync_report.end_compare(50, 50000)

    for i in six.moves.range(25):
        if i % 2 == 0:
            sync_report.print_completion('transferred: %d.txt' % i)
        sync_report.update_transfer(1, 1000)
        time.sleep(0.2)

    sync_report.close()
