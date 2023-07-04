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
            ] + list(range(25))
        )
        sync_report = SyncReport(stdout, False)
        sync_report.print_completion('transferred: 123.txt')

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
