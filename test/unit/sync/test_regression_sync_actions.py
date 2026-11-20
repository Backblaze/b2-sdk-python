######################################################################
#
# File: test/unit/sync/test_regression_sync_actions.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for sync actions and downloads to local files.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest
from apiver_deps import (
    B2Api,
    B2HttpApiConfig,
    InMemoryAccountInfo,
    InMemoryCache,
    RawSimulator,
    ServerDefaultSyncEncryptionSettingsProvider,
)
from apiver_deps_exception import ChecksumMismatch, TruncatedOutput

from b2sdk._internal.scan.path import B2Path
from b2sdk._internal.sync.action import B2CopyAction, B2DownloadAction
from test.known_defect import DefectStillPresent, expect, known_defect

pytestmark = [pytest.mark.apiver(from_ver=2)]

REMOTE_MTIME_MILLIS = 1_000_000_000_000  # 2001-09-09


@pytest.fixture
def api_and_bucket():
    api = B2Api(
        InMemoryAccountInfo(),
        cache=InMemoryCache(),
        api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
    )
    account_id, key = api.session.raw_api.create_account()
    api.authorize_account(application_key_id=account_id, application_key=key, realm='production')
    return api, api.create_bucket('sync-bucket', 'allPublic')


def _download_action(bucket, file_version, local_path):
    path = B2Path(file_version.file_name, file_version, [file_version])
    return B2DownloadAction(
        path,
        file_version.file_name,
        str(local_path),
        ServerDefaultSyncEncryptionSettingsProvider(),
    )


def _corrupt_stored_sha1(api, bucket, file_version):
    sim_bucket = api.session.raw_api.bucket_id_to_bucket[bucket.id_]
    sim_bucket.file_id_to_file[file_version.id_].content_sha1 = 'f' * 40


class TestDownloadedFileModTime:
    @known_defect(
        'C3',
        'a download that fails its checksum check still gets the remote modification time',
        raises=AssertionError,
    )
    def test_c3_failed_checksum_does_not_stamp_remote_mtime(self, api_and_bucket, tmp_path):
        """C3: a download that fails validation must not leave a file that looks current."""
        api, bucket = api_and_bucket
        fv = bucket.upload_bytes(
            b'hello world',
            'f.txt',
            file_info={'src_last_modified_millis': str(REMOTE_MTIME_MILLIS)},
        )
        _corrupt_stored_sha1(api, bucket, fv)
        target = tmp_path / 'out.txt'
        with pytest.raises((ChecksumMismatch, TruncatedOutput)):
            bucket.download_file_by_id(fv.id_).save_to(target)
        # either the file is gone, or it does not carry the remote mod time
        if target.exists():
            assert (
                int(target.stat().st_mtime * 1000) != REMOTE_MTIME_MILLIS
            ), 'the remote modification time was set on a file that failed validation'


class TestSyncDownloadAction:
    @known_defect(
        'C3', 'a failed sync download leaves the temporary file behind', raises=DefectStillPresent
    )
    def test_c3_failed_download_leaves_no_temp_file(self, api_and_bucket, tmp_path):
        """C3: after a failed sync download the .b2.sync.tmp file must not be left behind."""
        api, bucket = api_and_bucket
        fv = bucket.upload_bytes(b'hello world', 'f.txt')
        _corrupt_stored_sha1(api, bucket, fv)
        action = _download_action(bucket, fv, tmp_path / 'f.txt')
        with pytest.raises((ChecksumMismatch, TruncatedOutput)):
            action.do_action(bucket, None)
        assert not (tmp_path / 'f.txt').exists(), 'final file was created'
        expect(not (tmp_path / 'f.txt.b2.sync.tmp').exists(), 'temporary file left behind')

    @known_defect(
        'C17',
        'sync download deletes the old local file before renaming the new one into place',
        raises=DefectStillPresent,
    )
    def test_c17_failed_replacement_keeps_previous_local_copy(self, api_and_bucket, tmp_path):
        """C17: if moving the new file into place fails, the old local file must still exist.

        The fault is injected into both os.rename and os.replace, so the test does not depend on
        which of them the implementation uses to move the file into place.
        """
        api, bucket = api_and_bucket
        fv = bucket.upload_bytes(b'new content', 'f.txt')
        local = tmp_path / 'f.txt'
        local.write_bytes(b'old content')
        action = _download_action(bucket, fv, local)
        injected = []

        def fail(*args, **kwargs):
            injected.append(args)
            raise OSError('simulated replacement failure')

        with mock.patch('os.rename', side_effect=fail), mock.patch('os.replace', side_effect=fail):
            with pytest.raises(OSError, match='simulated replacement failure'):
                action.do_action(bucket, None)
        assert injected, 'the download never tried to move the new file into place'
        expect(local.exists(), 'the previous local copy was removed')
        expect(local.read_bytes() == b'old content', 'the previous local copy was changed')

    def test_c17_guard_successful_download_replaces_file(self, api_and_bucket, tmp_path):
        """Guard for C17: a normal sync download replaces the local file and leaves no temp file."""
        api, bucket = api_and_bucket
        fv = bucket.upload_bytes(b'new content', 'f.txt')
        local = tmp_path / 'f.txt'
        local.write_bytes(b'old content')
        _download_action(bucket, fv, local).do_action(bucket, None)
        assert local.read_bytes() == b'new content', 'the download did not replace the file'
        assert not os.path.exists(str(local) + '.b2.sync.tmp'), 'temporary file left behind'


class TestSyncMessages:
    NAME = 'dir/evil\x1b[31mname'

    def _path(self):
        return B2Path(self.NAME, mock.Mock(), [])

    @known_defect(
        'C24',
        'the sync download message prints remote names without escaping control characters',
        raises=AssertionError,
    )
    def test_c24_download_message_escapes_control_characters(self):
        """C24: remote names with control chars must be escaped in output."""
        action = B2DownloadAction(self._path(), self.NAME, 'out/x', mock.Mock())
        reporter = mock.Mock()
        action.do_report(mock.Mock(), reporter)
        printed = reporter.print_completion.call_args[0][0]
        assert '\x1b' not in printed, repr(printed)

    @known_defect(
        'C24',
        'the sync copy message prints remote names without escaping control characters',
        raises=AssertionError,
    )
    def test_c24_copy_message_escapes_control_characters(self):
        """C24: same for copy actions."""
        action = B2CopyAction(
            self.NAME, self._path(), 'dest', mock.Mock(), mock.Mock(), mock.Mock()
        )
        reporter = mock.Mock()
        action.do_report(mock.Mock(), reporter)
        printed = reporter.print_completion.call_args[0][0]
        assert '\x1b' not in printed, repr(printed)
