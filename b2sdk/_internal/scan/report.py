######################################################################
#
# File: b2sdk/_internal/scan/report.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from io import TextIOWrapper

from ..utils import format_and_scale_number
from ..utils.escape import escape_control_chars

logger = logging.getLogger(__name__)


@dataclass
class ProgressReport:
    """
    Handle reporting progress.

    This class is THREAD SAFE, so it can be used from parallel scan threads.
    """

    # Minimum time between displayed updates
    UPDATE_INTERVAL = 0.1

    stdout: TextIOWrapper  # standard output file object
    no_progress: bool  # if True, do not show progress

    def __post_init__(self):
        self.start_time = time.time()

        self.count = 0
        self.total_done = False
        self.total_count = 0

        self.closed = False
        self.lock = threading.Lock()
        self.current_line = ''
        self.encoding_warning_was_already_printed = False
        self._last_update_time = 0
        self._update_progress()
        self.warnings = []
        self.errors_encountered = False

    def close(self):
        """
        Perform a clean-up.
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

    def has_errors_or_warnings(self) -> bool:
        """
        Check if there are any errors or warnings.

        :return: True if there are any errors or warnings
        """
        return self.errors_encountered or bool(self.warnings)

    def error(self, message: str) -> None:
        """
        Print an error, gracefully interleaving it with a progress bar.

        :param message: an error message
        """
        self.print_completion(message)
        self.errors_encountered = True

    def print_completion(self, message: str) -> None:
        """
        Remove the progress bar, prints a message, and puts the progress
        bar back.

        :param message: an error message
        """
        with self.lock:
            self._print_line(message, True)
            self._last_update_time = 0
            self._update_progress()

    def update_count(self, delta: int) -> None:
        """
        Report that items have been processed.
        """
        with self.lock:
            self.count += delta
            self._update_progress()

    def _update_progress(self):
        if self.closed or self.no_progress:
            return

        now = time.time()
        interval = now - self._last_update_time
        if interval < self.UPDATE_INTERVAL:
            return

        self._last_update_time = now
        time_delta = time.time() - self.start_time
        rate = 0 if time_delta == 0 else int(self.count / time_delta)

        message = ' count: %d/%d   %s' % (
            self.count,
            self.total_count,
            format_and_scale_number(rate, '/s')
        )  # yapf: disable

        self._print_line(message, False)

    def _print_line(self, line: str, newline: bool) -> None:
        """
        Print a line to stdout.

        :param line: a string without a \r or \n in it.
        :param newline: True if the output should move to a new line after this one.
        """
        if len(line) < len(self.current_line):
            line += ' ' * (len(self.current_line) - len(line))
        try:
            self.stdout.write(line)
        except UnicodeEncodeError as encode_error:
            if not self.encoding_warning_was_already_printed:
                self.encoding_warning_was_already_printed = True
                self.stdout.write(
                    f'!WARNING! this terminal cannot properly handle progress reporting.  encoding is {self.stdout.encoding}.\n'
                )
            self.stdout.write(line.encode('ascii', 'backslashreplace').decode())
            logger.warning(
                f'could not output the following line with encoding {self.stdout.encoding} on stdout due to {encode_error}: {line}'
            )
        if newline:
            self.stdout.write('\n')
            self.current_line = ''
        else:
            self.stdout.write('\r')
            self.current_line = line
        self.stdout.flush()

    def update_total(self, delta: int) -> None:
        """
        Report that more files have been found for comparison.

        :param delta: number of files found since the last check
        """
        with self.lock:
            self.total_count += delta
            self._update_progress()

    def end_total(self) -> None:
        """
        Total files count is done. Can proceed to step 2.
        """
        with self.lock:
            self.total_done = True
            self._update_progress()

    def local_access_error(self, path: str) -> None:
        """
        Add a file access error message to the list of warnings.

        :param path: file path
        """
        self.warnings.append(
            f'WARNING: {escape_control_chars(path)} could not be accessed (broken symlink?)'
        )

    def local_permission_error(self, path: str) -> None:
        """
        Add a permission error message to the list of warnings.

        :param path: file path
        """
        self.warnings.append(
            f'WARNING: {escape_control_chars(path)} could not be accessed (no permissions to read?)'
        )

    def symlink_skipped(self, path: str) -> None:
        pass

    def circular_symlink_skipped(self, path: str) -> None:
        """
        Add a circular symlink error message to the list of warnings.

        :param path: file path
        """
        self.warnings.append(
            f'WARNING: {escape_control_chars(path)} is a circular symlink, which was already visited. Skipping.'
        )

    def invalid_name(self, path: str, error: str) -> None:
        """
        Add an invalid filename error message to the list of warnings.

        :param path: file path
        """
        self.warnings.append(
            f'WARNING: {escape_control_chars(path)} path contains invalid name ({error}). Skipping.'
        )


def sample_report_run():
    """
    Generate a sample report.
    """
    import sys
    report = ProgressReport(sys.stdout, False)

    for i in range(20):
        report.update_total(1)
        time.sleep(0.2)
        if i % 2 == 0:
            report.update_count(1)
    report.end_total()
    report.close()
