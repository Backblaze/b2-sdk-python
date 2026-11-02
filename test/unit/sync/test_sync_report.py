######################################################################
#
# File: test/unit/sync/test_sync_report.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from apiver_deps import SyncReport


class TestSyncReport:
    def test_bad_terminal(self):
        stdout = MagicMock()
        stdout.write = MagicMock(
            side_effect=[
                UnicodeEncodeError('codec', 'foo', 100, 105, 'artificial UnicodeEncodeError')
            ]
            + list(range(25))
        )
        sync_report = SyncReport(stdout, False)
        sync_report.print_completion('transferred: 123.txt')

    def test_bad_terminal_falls_back_to_escaped_output_and_warns_once(self):
        written = []

        class BadTerminal:
            encoding = 'ascii'

            def __init__(self):
                self.fail_next = True

            def write(self, text):
                if self.fail_next:
                    self.fail_next = False
                    raise UnicodeEncodeError('codec', 'foo', 100, 105, 'artificial')
                written.append(text)

            def flush(self):
                pass

        sync_report = SyncReport(BadTerminal(), True)
        sync_report.print_completion('transferred: caf\u00e9.txt')
        sync_report.print_completion('transferred: other.txt')

        output = ''.join(written)
        assert (
            output.count('!WARNING! this terminal cannot properly handle progress reporting') == 1
        )
        assert 'transferred: caf\\xe9.txt' in output
        assert 'transferred: other.txt' in output

    @pytest.mark.apiver(to_ver=1)
    def test_legacy_methods(self):
        stdout = MagicMock()
        sync_report = SyncReport(stdout, False)

        assert not sync_report.total_done
        assert not sync_report.local_done
        assert 0 == sync_report.total_count
        assert 0 == sync_report.local_file_count

        sync_report.local_done = True
        assert sync_report.local_done
        assert sync_report.total_done

        sync_report.local_file_count = 8
        assert 8 == sync_report.local_file_count
        assert 8 == sync_report.total_count

        sync_report.update_local(7)
        assert 15 == sync_report.total_count
        assert 15 == sync_report.local_file_count

        sync_report = SyncReport(stdout, False)
        assert not sync_report.total_done
        sync_report.end_local()
        assert sync_report.total_done
