######################################################################
#
# File: test/unit/bucket/test_bucket.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import contextlib
import dataclasses
import datetime
import io
import os
import pathlib
import platform
import tempfile
import time
import unittest.mock as mock
from contextlib import suppress
from io import BytesIO
from test.helpers import NonSeekableIO

import apiver_deps
import pytest
from apiver_deps_exception import (
    AccessDenied,
    AlreadyFailed,
    B2ConnectionError,
    B2Error,
    B2RequestTimeoutDuringUpload,
    BucketIdNotFound,
    DestinationDirectoryDoesntAllowOperation,
    DestinationDirectoryDoesntExist,
    DestinationIsADirectory,
    DestinationParentIsNotADirectory,
    DisablingFileLockNotSupported,
    FileSha1Mismatch,
    InvalidAuthToken,
    InvalidMetadataDirective,
    InvalidRange,
    InvalidUploadSource,
    MaxRetriesExceeded,
    RestrictedBucketMissing,
    SourceReplicationConflict,
    SSECKeyError,
    UnsatisfiableRange,
)

from ..test_base import TestBase, create_key

if apiver_deps.V <= 1:
    from apiver_deps import DownloadDestBytes, PreSeekedDownloadDest
    from apiver_deps import FileVersionInfo as VFileVersionInfo
else:
    DownloadDestBytes, PreSeekedDownloadDest = None, None  # these classes are not present, thus not needed, in v2
    from apiver_deps import FileVersion as VFileVersionInfo
from apiver_deps import (
    LARGE_FILE_SHA1,
    NO_RETENTION_FILE_SETTING,
    SSE_B2_AES,
    SSE_NONE,
    AbstractDownloader,
    AbstractProgressListener,
    B2Api,
    B2HttpApiConfig,
    B2Session,
    Bucket,
    BucketFactory,
    BucketRetentionSetting,
    BucketSimulator,
    CopySource,
    DownloadedFile,
    DownloadVersion,
    DummyCache,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FakeResponse,
    FileRetentionSetting,
    FileSimulator,
    InMemoryCache,
    LargeFileUploadState,
    LegalHold,
    MetadataDirectiveMode,
    ParallelDownloader,
    Part,
    Range,
    RawSimulator,
    ReplicationConfiguration,
    ReplicationRule,
    RetentionMode,
    RetentionPeriod,
    SimpleDownloader,
    StubAccountInfo,
    UploadMode,
    UploadSourceBytes,
    UploadSourceLocalFile,
    WriteIntent,
    hex_sha1_of_bytes,
)

pytestmark = [pytest.mark.apiver(from_ver=1)]

SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'some_key', key_id='some-id'),
)
SSE_C_AES_NO_SECRET = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=None, key_id='some-id'),
)
SSE_C_AES_2 = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'some_other_key', key_id='some-id-2'),
)
SSE_C_AES_2_NO_SECRET = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=None, key_id='some-id-2'),
)
SSE_C_AES_FROM_SERVER = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(key_id=None, secret=None),
)
REPLICATION = ReplicationConfiguration(
    rules=[
        ReplicationRule(
            destination_bucket_id='c5f35d53a90a7ea284fb0719',
            name='replication-us-west',
        ),
        ReplicationRule(
            destination_bucket_id='55f34d53a96a7ea284fb0719',
            name='replication-us-west-2',
            file_name_prefix='replica/',
            is_enabled=False,
            priority=255,
        ),
    ],
    source_key_id='10053d55ae26b790000000006',
    source_to_destination_key_mapping={
        "10053d55ae26b790000000045": "10053d55ae26b790000000004",
        "10053d55ae26b790000000046": "10053d55ae26b790030000004",
    },
)


def write_file(path, data):
    with open(path, 'wb') as f:
        f.write(data)


class StubProgressListener(AbstractProgressListener):
    """
    Implementation of a progress listener that remembers what calls were made,
    and returns them as a short string to use in unit tests.

    For a total byte count of 100, and updates at 33 and 66, the returned
    string looks like: "100: 33 66"
    """

    def __init__(self):
        self.total = None
        self.history = []
        self.last_byte_count = 0

    def get_history(self):
        return ' '.join(self.history)

    def set_total_bytes(self, total_byte_count):
        assert total_byte_count is not None
        assert self.total is None, 'set_total_bytes called twice'
        self.total = total_byte_count
        assert len(self.history) == 0, self.history
        self.history.append('%d:' % (total_byte_count,))

    def bytes_completed(self, byte_count):
        self.last_byte_count = byte_count
        self.history.append(str(byte_count))

    def is_valid(self, **kwargs):
        valid, _ = self.is_valid_reason(**kwargs)
        return valid

    def is_valid_reason(self, check_progress=True, check_monotonic_progress=False):
        progress_end = -1
        if self.history[progress_end] == 'closed':
            progress_end = -2

        # self.total != self.last_byte_count may be a consequence of non-monotonic
        # progress, so we want to check this first
        if check_monotonic_progress:
            prev = 0
            for val in map(int, self.history[1:progress_end]):
                if val < prev:
                    return False, 'non-monotonic progress'
                prev = val
        if self.total != self.last_byte_count:
            return False, 'total different than last_byte_count'
        if check_progress and len(self.history[1:progress_end]) < 2:
            return False, 'progress in history has less than 2 entries'
        return True, ''

    def close(self):
        self.history.append('closed')


class CanRetry(B2Error):
    """
    An exception that can be retryable, or not.
    """

    def __init__(self, can_retry):
        super().__init__(None, None, None, None, None)
        self.can_retry = can_retry

    def should_retry_upload(self):
        return self.can_retry


def bucket_ls(bucket, *args, show_versions=False, **kwargs):
    if apiver_deps.V <= 1:
        ls_all_versions_kwarg = {'show_versions': show_versions}
    else:
        ls_all_versions_kwarg = {'latest_only': not show_versions}
    return bucket.ls(*args, **ls_all_versions_kwarg, **kwargs)


class TestCaseWithBucket(TestBase):
    RAW_SIMULATOR_CLASS = RawSimulator
    CACHE_CLASS = DummyCache

    def get_api(self):
        return B2Api(
            self.account_info,
            cache=self.CACHE_CLASS(),
            api_config=B2HttpApiConfig(_raw_api_class=self.RAW_SIMULATOR_CLASS),
        )

    def setUp(self):
        self.bucket_name = 'my-bucket'
        self.account_info = StubAccountInfo()
        self.api = self.get_api()
        self.simulator = self.api.session.raw_api
        (self.account_id, self.master_key) = self.simulator.create_account()
        self.api.authorize_account(
            application_key_id=self.account_id,
            application_key=self.master_key,
            realm='production',
        )
        self.api_url = self.account_info.get_api_url()
        self.account_auth_token = self.account_info.get_account_auth_token()
        self.bucket = self.api.create_bucket(self.bucket_name, 'allPublic')
        self.bucket_id = self.bucket.id_

    def bucket_ls(self, *args, show_versions=False, **kwargs):
        return bucket_ls(self.bucket, *args, show_versions=show_versions, **kwargs)

    def assertBucketContents(self, expected, *args, **kwargs):
        """
        *args and **kwargs are passed to self.bucket_ls()
        """
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls(*args, **kwargs)
        ]
        self.assertEqual(expected, actual)

    def _make_data(self, approximate_length):
        """
        Generate a sequence of bytes to use in testing an upload.
        Don't repeat a short pattern, so we're sure that the different
        parts of a large file are actually different.

        Returns bytes.
        """
        fragments = []
        so_far = 0
        while so_far < approximate_length:
            fragment = ('%d:' % so_far).encode('utf-8')
            so_far += len(fragment)
            fragments.append(fragment)
        return b''.join(fragments)

    def _check_file_contents(self, file_name, expected_contents):
        contents = self._download_file(file_name)
        self.assertEqual(expected_contents, contents)

    def _check_large_file_sha1(self, file_name, expected_sha1):
        file_info = self.bucket.get_file_info_by_name(file_name).file_info
        if expected_sha1:
            assert LARGE_FILE_SHA1 in file_info
            assert file_info[LARGE_FILE_SHA1] == expected_sha1
        else:
            assert LARGE_FILE_SHA1 not in file_info

    def _download_file(self, file_name):
        with FileSimulator.dont_check_encryption():
            if apiver_deps.V <= 1:
                download = DownloadDestBytes()
                self.bucket.download_file_by_name(file_name, download)
                return download.get_bytes_written()
            else:
                with io.BytesIO() as bytes_io:
                    downloaded_file = self.bucket.download_file_by_name(file_name)
                    downloaded_file.save(bytes_io)
                    return bytes_io.getvalue()


class TestReauthorization(TestCaseWithBucket):
    def testCreateBucket(self):
        class InvalidAuthTokenWrapper:
            def __init__(self, original_function):
                self.__original_function = original_function
                self.__name__ = original_function.__name__
                self.__called = False

            def __call__(self, *args, **kwargs):
                if self.__called:
                    return self.__original_function(*args, **kwargs)
                self.__called = True
                raise InvalidAuthToken('message', 401)

        self.simulator.create_bucket = InvalidAuthTokenWrapper(self.simulator.create_bucket)
        self.bucket = self.api.create_bucket('your-bucket', 'allPublic')


class TestListParts(TestCaseWithBucket):
    @pytest.mark.apiver(to_ver=1)
    def testEmpty(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        self.assertEqual([], list(self.bucket.list_parts(file1.file_id, batch_size=1)))

    @pytest.mark.apiver(to_ver=1)
    def testThree(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        content = b'hello world'
        content_sha1 = hex_sha1_of_bytes(content)
        large_file_upload_state = mock.MagicMock()
        large_file_upload_state.has_error.return_value = False
        self.api.services.upload_manager.upload_part(
            self.bucket_id, file1.file_id, UploadSourceBytes(content), 1, large_file_upload_state
        ).result()
        self.api.services.upload_manager.upload_part(
            self.bucket_id, file1.file_id, UploadSourceBytes(content), 2, large_file_upload_state
        ).result()
        self.api.services.upload_manager.upload_part(
            self.bucket_id, file1.file_id, UploadSourceBytes(content), 3, large_file_upload_state
        ).result()
        expected_parts = [
            Part('9999', 1, 11, content_sha1),
            Part('9999', 2, 11, content_sha1),
            Part('9999', 3, 11, content_sha1),
        ]
        self.assertEqual(expected_parts, list(self.bucket.list_parts(file1.file_id, batch_size=1)))


class TestUploadPart(TestCaseWithBucket):
    @pytest.mark.apiver(to_ver=1)
    def test_error_in_state(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        content = b'hello world'
        file_progress_listener = mock.MagicMock()
        large_file_upload_state = LargeFileUploadState(file_progress_listener)
        large_file_upload_state.set_error('test error')
        try:
            self.api.services.upload_manager.upload_part(
                self.bucket_id, file1.file_id, UploadSourceBytes(content), 1,
                large_file_upload_state
            ).result()
            self.fail('should have thrown')
        except AlreadyFailed:
            pass


class TestListUnfinished(TestCaseWithBucket):
    def test_empty(self):
        self.assertEqual([], list(self.bucket.list_unfinished_large_files()))

    @pytest.mark.apiver(to_ver=1)
    def test_one(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        self.assertEqual([file1], list(self.bucket.list_unfinished_large_files()))

    @pytest.mark.apiver(to_ver=1)
    def test_three(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        file2 = self.bucket.start_large_file('file2.txt', 'text/plain', {})
        file3 = self.bucket.start_large_file('file3.txt', 'text/plain', {})
        self.assertEqual(
            [file1, file2, file3], list(self.bucket.list_unfinished_large_files(batch_size=1))
        )

    @pytest.mark.apiver(to_ver=1)
    def test_prefix(self):
        self.bucket.start_large_file('fileA', 'text/plain', {})
        file2 = self.bucket.start_large_file('fileAB', 'text/plain', {})
        file3 = self.bucket.start_large_file('fileABC', 'text/plain', {})
        self.assertEqual(
            [file2, file3],
            list(self.bucket.list_unfinished_large_files(
                batch_size=1,
                prefix='fileAB',
            ),),
        )

    def _make_file(self, file_id, file_name):
        return self.bucket.start_large_file(file_name, 'text/plain', {})


class TestGetFileInfo(TestCaseWithBucket):
    def test_version_by_name(self):
        data = b'hello world'
        a_id = self.bucket.upload_bytes(data, 'a').id_

        info = self.bucket.get_file_info_by_name('a')

        if apiver_deps.V <= 1:
            self.assertIsInstance(info, VFileVersionInfo)
        else:
            self.assertIsInstance(info, DownloadVersion)
        expected = (a_id, 'a', 11, 'b2/x-auto', 'none', NO_RETENTION_FILE_SETTING, LegalHold.UNSET)
        actual = (
            info.id_,
            info.file_name,
            info.size,
            info.content_type,
            info.server_side_encryption.mode.value,
            info.file_retention,
            info.legal_hold,
        )
        self.assertEqual(expected, actual)

    def test_version_by_name_file_lock(self):
        bucket = self.api.create_bucket(
            'my-bucket-with-file-lock', 'allPublic', is_file_lock_enabled=True
        )
        data = b'hello world'
        legal_hold = LegalHold.ON
        file_retention = FileRetentionSetting(RetentionMode.COMPLIANCE, 100)

        bucket.upload_bytes(data, 'a', file_retention=file_retention, legal_hold=legal_hold)

        file_version = bucket.get_file_info_by_name('a')

        actual = (file_version.legal_hold, file_version.file_retention)
        self.assertEqual((legal_hold, file_retention), actual)

        low_perm_account_info = StubAccountInfo()
        low_perm_api = B2Api(low_perm_account_info)
        low_perm_api.session.raw_api = self.simulator
        low_perm_key = create_key(
            self.api,
            key_name='lowperm',
            capabilities=[
                'listKeys',
                'listBuckets',
                'listFiles',
                'readFiles',
            ]
        )

        low_perm_api.authorize_account(
            application_key_id=low_perm_key.id_,
            application_key=low_perm_key.application_key,
            realm='production',
        )
        low_perm_bucket = low_perm_api.get_bucket_by_name('my-bucket-with-file-lock')

        file_version = low_perm_bucket.get_file_info_by_name('a')

        actual = (file_version.legal_hold, file_version.file_retention)
        expected = (LegalHold.UNKNOWN, FileRetentionSetting(RetentionMode.UNKNOWN))
        self.assertEqual(expected, actual)

    def test_version_by_id(self):
        data = b'hello world'
        b_id = self.bucket.upload_bytes(data, 'b').id_

        info = self.bucket.get_file_info_by_id(b_id)

        self.assertIsInstance(info, VFileVersionInfo)
        expected = (b_id, 'b', 11, 'upload', 'b2/x-auto', 'none')
        actual = (
            info.id_, info.file_name, info.size, info.action, info.content_type,
            info.server_side_encryption.mode.value
        )
        self.assertEqual(expected, actual)


class TestLs(TestCaseWithBucket):
    def test_empty(self):
        self.assertEqual([], list(self.bucket_ls('foo')))

    def test_one_file_at_root(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'hello.txt')
        expected = [('hello.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '')

    def test_three_files_at_root(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'bb')
        self.bucket.upload_bytes(data, 'ccc')
        expected = [
            ('a', 11, 'upload', None), ('bb', 11, 'upload', None), ('ccc', 11, 'upload', None)
        ]
        self.assertBucketContents(expected, '')

    def test_three_files_in_dir(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'bb/1')
        self.bucket.upload_bytes(data, 'bb/2/sub1')
        self.bucket.upload_bytes(data, 'bb/2/sub2')
        self.bucket.upload_bytes(data, 'bb/3')
        self.bucket.upload_bytes(data, 'ccc')
        expected = [
            ('bb/1', 11, 'upload', None), ('bb/2/sub1', 11, 'upload', 'bb/2/'),
            ('bb/3', 11, 'upload', None)
        ]
        self.assertBucketContents(expected, 'bb', fetch_count=1)

    def test_three_files_multiple_versions(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'bb/1')
        self.bucket.upload_bytes(data, 'bb/2')
        self.bucket.upload_bytes(data, 'bb/2')
        self.bucket.upload_bytes(data, 'bb/2')
        self.bucket.upload_bytes(data, 'bb/3')
        self.bucket.upload_bytes(data, 'ccc')
        expected = [
            ('9998', 'bb/1', 11, 'upload', None),
            ('9995', 'bb/2', 11, 'upload', None),
            ('9996', 'bb/2', 11, 'upload', None),
            ('9997', 'bb/2', 11, 'upload', None),
            ('9994', 'bb/3', 11, 'upload', None),
        ]
        actual = [
            (info.id_, info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('bb', show_versions=True, fetch_count=1)
        ]
        self.assertEqual(expected, actual)

    @pytest.mark.apiver(to_ver=1)
    def test_started_large_file(self):
        self.bucket.start_large_file('hello.txt')
        expected = [('hello.txt', 0, 'start', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    def test_hidden_file(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'hello.txt')
        self.bucket.hide_file('hello.txt')
        expected = [('hello.txt', 0, 'hide', None), ('hello.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    def test_delete_file_version(self):
        data = b'hello world'

        file_id = self.bucket.upload_bytes(data, 'hello.txt').id_

        data = b'hello new world'
        self.bucket.upload_bytes(data, 'hello.txt')
        self.bucket.delete_file_version(file_id, 'hello.txt')

        expected = [('hello.txt', 15, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    def test_delete_file_version_bypass_governance(self):
        data = b'hello world'

        file_id = self.bucket.upload_bytes(
            data,
            'hello.txt',
            file_retention=FileRetentionSetting(RetentionMode.GOVERNANCE,
                                                int(time.time()) + 100),
        ).id_

        with pytest.raises(AccessDenied):
            self.bucket.delete_file_version(file_id, 'hello.txt')

        self.bucket.delete_file_version(file_id, 'hello.txt', bypass_governance=True)
        self.assertBucketContents([], '', show_versions=True)

    def test_non_recursive_returns_folder_names(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/1/test-1.txt')
        self.bucket.upload_bytes(data, 'b/2/test-2.txt')
        self.bucket.upload_bytes(data, 'b/3/test-3.txt')
        self.bucket.upload_bytes(data, 'b/3/test-4.txt')
        # Since inside `b` there are 3 directories, we get three results,
        # with a first file for each of them.
        expected = [
            ('b/1/test-1.txt', len(data), 'upload', 'b/1/'),
            ('b/2/test-2.txt', len(data), 'upload', 'b/2/'),
            ('b/3/test-3.txt', len(data), 'upload', 'b/3/'),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('b/')
        ]
        self.assertEqual(expected, actual)

    def test_recursive_returns_no_folder_names(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/1/test-1.txt')
        self.bucket.upload_bytes(data, 'b/2/test-2.txt')
        self.bucket.upload_bytes(data, 'b/3/test-3.txt')
        self.bucket.upload_bytes(data, 'b/3/test-4.txt')
        expected = [
            ('b/1/test-1.txt', len(data), 'upload', None),
            ('b/2/test-2.txt', len(data), 'upload', None),
            ('b/3/test-3.txt', len(data), 'upload', None),
            ('b/3/test-4.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('b/', recursive=True)
        ]
        self.assertEqual(expected, actual)

    def test_wildcard_matching(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/1/test-1.txt')
        self.bucket.upload_bytes(data, 'b/2/test-2.csv')
        self.bucket.upload_bytes(data, 'b/2/test-3.txt')
        self.bucket.upload_bytes(data, 'b/3/test-4.jpg')
        self.bucket.upload_bytes(data, 'b/3/test-4.txt')
        self.bucket.upload_bytes(data, 'b/3/test-5.txt')
        expected = [
            ('b/1/test-1.txt', len(data), 'upload', None),
            ('b/2/test-3.txt', len(data), 'upload', None),
            ('b/3/test-4.txt', len(data), 'upload', None),
            ('b/3/test-5.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('b/*.txt', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_wildcard_matching_including_root(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'b/1/test.txt')
        self.bucket.upload_bytes(data, 'b/2/test.txt')
        self.bucket.upload_bytes(data, 'b/3/test.txt')
        self.bucket.upload_bytes(data, 'test.txt')
        expected = [
            ('b/1/test.txt', len(data), 'upload', None),
            ('b/2/test.txt', len(data), 'upload', None),
            ('b/3/test.txt', len(data), 'upload', None),
            ('test.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('*.txt', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_wildcard_matching_directory(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/2/test.txt')
        self.bucket.upload_bytes(data, 'b/3/test.jpg')
        self.bucket.upload_bytes(data, 'b/3/test.txt')
        self.bucket.upload_bytes(data, 'c/4/test.txt')
        expected = [
            ('b/2/test.txt', len(data), 'upload', None),
            ('b/3/test.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info,
                 folder) in self.bucket_ls('b/*/test.txt', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_single_character_matching(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/2/test.csv')
        self.bucket.upload_bytes(data, 'b/2/test.txt')
        self.bucket.upload_bytes(data, 'b/2/test.tsv')
        expected = [
            ('b/2/test.csv', len(data), 'upload', None),
            ('b/2/test.tsv', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info,
                 folder) in self.bucket_ls('b/2/test.?sv', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_sequence_matching(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/2/test.csv')
        self.bucket.upload_bytes(data, 'b/2/test.ksv')
        self.bucket.upload_bytes(data, 'b/2/test.tsv')
        expected = [
            ('b/2/test.csv', len(data), 'upload', None),
            ('b/2/test.tsv', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info,
                 folder) in self.bucket_ls('b/2/test.[tc]sv', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_negative_sequence_matching(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'b/2/test.csv')
        self.bucket.upload_bytes(data, 'b/2/test.ksv')
        self.bucket.upload_bytes(data, 'b/2/test.tsv')
        expected = [
            ('b/2/test.tsv', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info,
                 folder) in self.bucket_ls('b/2/test.[!ck]sv', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_matching_wildcard_named_file(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'a/*.txt')
        self.bucket.upload_bytes(data, 'a/1.txt')
        self.bucket.upload_bytes(data, 'a/2.txt')
        expected = [
            ('a/*.txt', len(data), 'upload', None),
            ('a/1.txt', len(data), 'upload', None),
            ('a/2.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('a/*.txt', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_matching_single_question_mark_named_file(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'b/?.txt')
        self.bucket.upload_bytes(data, 'b/a.txt')
        self.bucket.upload_bytes(data, 'b/b.txt')
        expected = [
            ('b/?.txt', len(data), 'upload', None),
            ('b/a.txt', len(data), 'upload', None),
            ('b/b.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('b/?.txt', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)

    def test_wildcard_requires_recursive(self):
        with pytest.raises(ValueError):
            # Since ls is a generator, we need to actually fetch something from it.
            next(self.bucket_ls('*.txt', recursive=False, with_wildcard=True))

    def test_matching_exact_filename(self):
        data = b'hello world'
        self.bucket.upload_bytes(data, 'b/a.txt')
        self.bucket.upload_bytes(data, 'b/b.txt')
        expected = [
            ('b/a.txt', len(data), 'upload', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket_ls('b/a.txt', recursive=True, with_wildcard=True)
        ]
        self.assertEqual(expected, actual)


class TestGetFreshState(TestCaseWithBucket):
    def test_ok(self):
        same_but_different = self.api.get_bucket_by_id(self.bucket.id_)
        same_but_different = same_but_different.get_fresh_state()
        assert isinstance(same_but_different, Bucket)
        assert id(same_but_different) != id(self.bucket)
        assert same_but_different.as_dict() == self.bucket.as_dict()
        same_but_different = same_but_different.update(bucket_info={'completely': 'new info'})
        if apiver_deps.V <= 1:
            same_but_different = BucketFactory.from_api_bucket_dict(self.api, same_but_different)
        assert same_but_different.as_dict() != self.bucket.as_dict()
        refreshed_bucket = self.bucket.get_fresh_state()
        assert same_but_different.as_dict() == refreshed_bucket.as_dict()

    def test_fail(self):
        self.api.delete_bucket(self.bucket)
        with pytest.raises(BucketIdNotFound):
            self.bucket.get_fresh_state()


class TestListVersions(TestCaseWithBucket):
    def test_single_version(self):
        data = b'hello world'
        a_id = self.bucket.upload_bytes(data, 'a').id_
        b_id = self.bucket.upload_bytes(data, 'b').id_
        c_id = self.bucket.upload_bytes(data, 'c').id_

        expected = [(a_id, 'a', 11, 'upload')]
        actual = [
            (info.id_, info.file_name, info.size, info.action)
            for info in self.bucket.list_file_versions('a')
        ]
        self.assertEqual(expected, actual)

        expected = [(b_id, 'b', 11, 'upload')]
        actual = [
            (info.id_, info.file_name, info.size, info.action)
            for info in self.bucket.list_file_versions('b')
        ]
        self.assertEqual(expected, actual)

        expected = [(c_id, 'c', 11, 'upload')]
        actual = [
            (info.id_, info.file_name, info.size, info.action)
            for info in self.bucket.list_file_versions('c')
        ]
        self.assertEqual(expected, actual)

    def test_multiple_version(self):
        a_id1 = self.bucket.upload_bytes(b'first version', 'a').id_
        a_id2 = self.bucket.upload_bytes(b'second version', 'a').id_
        a_id3 = self.bucket.upload_bytes(b'last version', 'a').id_

        expected = [
            (a_id3, 'a', 12, 'upload'), (a_id2, 'a', 14, 'upload'), (a_id1, 'a', 13, 'upload')
        ]
        actual = [
            (info.id_, info.file_name, info.size, info.action)
            for info in self.bucket.list_file_versions('a')
        ]
        self.assertEqual(expected, actual)

    def test_ignores_subdirectory(self):
        data = b'hello world'
        file_id = self.bucket.upload_bytes(data, 'a/b').id_
        self.bucket.upload_bytes(data, 'a/b/c')

        expected = [(file_id, 'a/b', 11, 'upload')]
        actual = [
            (info.id_, info.file_name, info.size, info.action)
            for info in self.bucket.list_file_versions('a/b')
        ]
        self.assertEqual(expected, actual)

    def test_all_versions_in_response(self):
        data = b'hello world'
        file_id = self.bucket.upload_bytes(data, 'a/b').id_
        self.bucket.upload_bytes(data, 'a/b/c')

        expected = [(file_id, 'a/b', 11, 'upload')]
        actual = [
            (info.id_, info.file_name, info.size, info.action)
            for info in self.bucket.list_file_versions('a/b', fetch_count=1)
        ]
        self.assertEqual(expected, actual)

    def test_bad_fetch_count(self):
        try:
            # Convert to a list to cause the generator to execute.
            list(self.bucket.list_file_versions('a', fetch_count=0))
            self.fail('should have raised ValueError')
        except ValueError as e:
            self.assertEqual('unsupported fetch_count value', str(e))

    def test_encryption(self):
        data = b'hello world'
        a = self.bucket.upload_bytes(data, 'a')
        a_id = a.id_
        self.assertEqual(a.server_side_encryption, SSE_NONE)
        b = self.bucket.upload_bytes(data, 'b', encryption=SSE_B2_AES)
        self.assertEqual(b.server_side_encryption, SSE_B2_AES)
        b_id = b.id_
        # c_id = self.bucket.upload_bytes(data, 'c', encryption=SSE_NONE).id_  # TODO
        self.bucket.copy(a_id, 'd', destination_encryption=SSE_B2_AES)
        self.bucket.copy(
            b_id, 'e', destination_encryption=SSE_C_AES, file_info={}, content_type='text/plain'
        )

        actual = [info.server_side_encryption for info in self.bucket.list_file_versions('a')][0]
        self.assertEqual(SSE_NONE, actual)  # bucket default
        actual = self.bucket.get_file_info_by_name('a').server_side_encryption
        self.assertEqual(SSE_NONE, actual)  # bucket default

        actual = [info.server_side_encryption for info in self.bucket.list_file_versions('b')][0]
        self.assertEqual(SSE_B2_AES, actual)  # explicitly requested sse-b2
        actual = self.bucket.get_file_info_by_name('b').server_side_encryption
        self.assertEqual(SSE_B2_AES, actual)  # explicitly requested sse-b2

        # actual = [info.server_side_encryption for info in self.bucket.list_file_versions('c')][0]
        # self.assertEqual(SSE_NONE, actual)  # explicitly requested none

        actual = [info.server_side_encryption for info in self.bucket.list_file_versions('d')][0]
        self.assertEqual(SSE_B2_AES, actual)  # explicitly requested sse-b2
        actual = self.bucket.get_file_info_by_name('d').server_side_encryption
        self.assertEqual(SSE_B2_AES, actual)  # explicitly requested sse-b2

        actual = [info.server_side_encryption for info in self.bucket.list_file_versions('e')][0]
        self.assertEqual(SSE_C_AES_NO_SECRET, actual)  # explicitly requested sse-c
        actual = self.bucket.get_file_info_by_name('e').server_side_encryption
        self.assertEqual(SSE_C_AES_NO_SECRET, actual)  # explicitly requested sse-c


class TestCopyFile(TestCaseWithBucket):
    @classmethod
    def _copy_function(cls, bucket):
        if apiver_deps.V <= 1:
            return bucket.copy_file
        else:
            return bucket.copy

    @pytest.mark.apiver(from_ver=2)
    def test_copy_big(self):
        data = b'HelloWorld' * 100
        for i in range(10):
            data += bytes(':#' + str(i) + '$' + 'abcdefghijklmnopqrstuvwx' * 4, 'ascii')
        file_info = self.bucket.upload_bytes(data, 'file1')

        self.bucket.copy(
            file_info.id_,
            'file2',
            min_part_size=200,
            max_part_size=400,
        )
        self._check_file_contents('file2', data)

    def test_copy_without_optional_params(self):
        file_id = self._make_file()
        if apiver_deps.V <= 1:
            f = self.bucket.copy_file(file_id, 'hello_new.txt')
            assert f['action'] == 'copy'
        else:
            f = self.bucket.copy(file_id, 'hello_new.txt')
            assert f.action == 'copy'
        expected = [('hello.txt', 11, 'upload', None), ('hello_new.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    def test_copy_with_range(self):
        file_id = self._make_file()
        #data = b'hello world'
        #            3456789
        if apiver_deps.V <= 1:
            self.bucket.copy_file(
                file_id,
                'hello_new.txt',
                bytes_range=(3, 9),
            )  # inclusive, confusingly
        else:
            self.bucket.copy(file_id, 'hello_new.txt', offset=3, length=7)
        self._check_file_contents('hello_new.txt', b'lo worl')
        self._check_large_file_sha1('hello_new.txt', None)
        expected = [('hello.txt', 11, 'upload', None), ('hello_new.txt', 7, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    @pytest.mark.apiver(to_ver=1)
    def test_copy_with_invalid_metadata(self):
        file_id = self._make_file()
        try:
            self.bucket.copy_file(
                file_id,
                'hello_new.txt',
                metadata_directive=MetadataDirectiveMode.COPY,
                content_type='application/octet-stream',
            )
            self.fail('should have raised InvalidMetadataDirective')
        except InvalidMetadataDirective as e:
            self.assertEqual(
                'content_type and file_info should be None when metadata_directive is COPY',
                str(e),
            )
        expected = [('hello.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    @pytest.mark.apiver(to_ver=1)
    def test_copy_with_invalid_metadata_replace(self):
        file_id = self._make_file()
        try:
            self.bucket.copy_file(
                file_id,
                'hello_new.txt',
                metadata_directive=MetadataDirectiveMode.REPLACE,
            )
            self.fail('should have raised InvalidMetadataDirective')
        except InvalidMetadataDirective as e:
            self.assertEqual(
                'content_type cannot be None when metadata_directive is REPLACE',
                str(e),
            )
        expected = [('hello.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    @pytest.mark.apiver(to_ver=1)
    def test_copy_with_replace_metadata(self):
        file_id = self._make_file()
        self.bucket.copy_file(
            file_id,
            'hello_new.txt',
            metadata_directive=MetadataDirectiveMode.REPLACE,
            content_type='text/plain',
        )
        expected = [
            ('hello.txt', 11, 'upload', 'b2/x-auto', None),
            ('hello_new.txt', 11, 'upload', 'text/plain', None),
        ]
        actual = [
            (info.file_name, info.size, info.action, info.content_type, folder)
            for (info, folder) in self.bucket_ls(show_versions=True)
        ]
        self.assertEqual(expected, actual)

    def test_copy_with_unsatisfied_range(self):
        file_id = self._make_file()
        try:
            if apiver_deps.V <= 1:
                self.bucket.copy_file(
                    file_id,
                    'hello_new.txt',
                    bytes_range=(12, 15),
                )
            else:
                self.bucket.copy(
                    file_id,
                    'hello_new.txt',
                    offset=12,
                    length=3,
                )
            self.fail('should have raised UnsatisfiableRange')
        except UnsatisfiableRange as e:
            self.assertEqual(
                'The range in the request is outside the size of the file',
                str(e),
            )
        expected = [('hello.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    def test_copy_with_different_bucket(self):
        source_bucket = self.api.create_bucket('source-bucket', 'allPublic')
        file_id = self._make_file(source_bucket)
        self._copy_function(self.bucket)(file_id, 'hello_new.txt')

        def ls(bucket):
            return [
                (info.file_name, info.size, info.action, folder)
                for (info, folder) in bucket_ls(bucket, show_versions=True)
            ]

        expected = [('hello.txt', 11, 'upload', None)]
        self.assertEqual(expected, ls(source_bucket))
        expected = [('hello_new.txt', 11, 'upload', None)]
        self.assertBucketContents(expected, '', show_versions=True)

    def test_copy_retention(self):
        for data in [self._make_data(self.simulator.MIN_PART_SIZE * 3), b'hello']:
            for length in [None, len(data)]:
                with self.subTest(real_length=len(data), length=length):
                    file_id = self.bucket.upload_bytes(data, 'original_file').id_
                    resulting_file_version = self.bucket.copy(
                        file_id,
                        'copied_file',
                        file_retention=FileRetentionSetting(RetentionMode.COMPLIANCE, 100),
                        legal_hold=LegalHold.ON,
                        max_part_size=400,
                    )
                    self.assertEqual(
                        FileRetentionSetting(RetentionMode.COMPLIANCE, 100),
                        resulting_file_version.file_retention
                    )
                    self.assertEqual(LegalHold.ON, resulting_file_version.legal_hold)

    def test_copy_encryption(self):
        data = b'hello_world'
        a = self.bucket.upload_bytes(data, 'a')
        a_id = a.id_
        self.assertEqual(a.server_side_encryption, SSE_NONE)

        b = self.bucket.upload_bytes(data, 'b', encryption=SSE_B2_AES)
        self.assertEqual(b.server_side_encryption, SSE_B2_AES)
        b_id = b.id_

        c = self.bucket.upload_bytes(data, 'c', encryption=SSE_C_AES)
        self.assertEqual(c.server_side_encryption, SSE_C_AES_NO_SECRET)
        c_id = c.id_

        for length in [None, len(data)]:
            for kwargs, expected_encryption in [
                (dict(file_id=a_id, destination_encryption=SSE_B2_AES), SSE_B2_AES),
                (
                    dict(
                        file_id=a_id,
                        destination_encryption=SSE_C_AES,
                        file_info={'new': 'value'},
                        content_type='text/plain'
                    ), SSE_C_AES_NO_SECRET
                ),
                (
                    dict(
                        file_id=a_id,
                        destination_encryption=SSE_C_AES,
                        source_file_info={'old': 'value'},
                        source_content_type='text/plain'
                    ), SSE_C_AES_NO_SECRET
                ),
                (dict(file_id=b_id), SSE_NONE),
                (dict(file_id=b_id, source_encryption=SSE_B2_AES), SSE_NONE),
                (
                    dict(
                        file_id=b_id,
                        source_encryption=SSE_B2_AES,
                        destination_encryption=SSE_B2_AES
                    ), SSE_B2_AES
                ),
                (
                    dict(
                        file_id=b_id,
                        source_encryption=SSE_B2_AES,
                        destination_encryption=SSE_C_AES,
                        file_info={'new': 'value'},
                        content_type='text/plain'
                    ), SSE_C_AES_NO_SECRET
                ),
                (
                    dict(
                        file_id=b_id,
                        source_encryption=SSE_B2_AES,
                        destination_encryption=SSE_C_AES,
                        source_file_info={'old': 'value'},
                        source_content_type='text/plain'
                    ), SSE_C_AES_NO_SECRET
                ),
                (
                    dict(
                        file_id=c_id,
                        source_encryption=SSE_C_AES,
                        file_info={'new': 'value'},
                        content_type='text/plain'
                    ), SSE_NONE
                ),
                (
                    dict(
                        file_id=c_id,
                        source_encryption=SSE_C_AES,
                        source_file_info={'old': 'value'},
                        source_content_type='text/plain'
                    ), SSE_NONE
                ),
                (
                    dict(
                        file_id=c_id, source_encryption=SSE_C_AES, destination_encryption=SSE_C_AES
                    ), SSE_C_AES_NO_SECRET
                ),
                (
                    dict(
                        file_id=c_id,
                        source_encryption=SSE_C_AES,
                        destination_encryption=SSE_B2_AES,
                        source_file_info={'old': 'value'},
                        source_content_type='text/plain'
                    ), SSE_B2_AES
                ),
                (
                    dict(
                        file_id=c_id,
                        source_encryption=SSE_C_AES,
                        destination_encryption=SSE_B2_AES,
                        file_info={'new': 'value'},
                        content_type='text/plain'
                    ), SSE_B2_AES
                ),
                (
                    dict(
                        file_id=c_id,
                        source_encryption=SSE_C_AES,
                        destination_encryption=SSE_C_AES_2,
                        source_file_info={'old': 'value'},
                        source_content_type='text/plain'
                    ), SSE_C_AES_2_NO_SECRET
                ),
                (
                    dict(
                        file_id=c_id,
                        source_encryption=SSE_C_AES,
                        destination_encryption=SSE_C_AES_2,
                        file_info={'new': 'value'},
                        content_type='text/plain'
                    ), SSE_C_AES_2_NO_SECRET
                ),
            ]:
                with self.subTest(kwargs=kwargs, length=length, data=data):
                    file_info = self.bucket.copy(**kwargs, new_file_name='new_file', length=length)
                    self.assertTrue(isinstance(file_info, VFileVersionInfo))
                    self.assertEqual(file_info.server_side_encryption, expected_encryption)

    def _make_file(self, bucket=None):
        data = b'hello world'
        actual_bucket = bucket or self.bucket
        return actual_bucket.upload_bytes(data, 'hello.txt').id_


class TestUpdate(TestCaseWithBucket):
    def test_update(self):
        result = self.bucket.update(
            bucket_type='allPrivate',
            bucket_info={'info': 'o'},
            cors_rules={'andrea': 'corr'},
            lifecycle_rules=[{
                'fileNamePrefix': 'is_life'
            }],
            default_server_side_encryption=SSE_B2_AES,
            default_retention=BucketRetentionSetting(
                RetentionMode.COMPLIANCE, RetentionPeriod(years=7)
            ),
            replication=REPLICATION,
        )
        if apiver_deps.V <= 1:
            self.maxDiff = None
            with suppress(KeyError):
                del result['replicationConfiguration']
            self.assertEqual(
                {
                    'accountId': 'account-0',
                    'bucketId': 'bucket_0',
                    'bucketInfo': {
                        'info': 'o'
                    },
                    'bucketName': 'my-bucket',
                    'bucketType': 'allPrivate',
                    'corsRules': {
                        'andrea': 'corr'
                    },
                    'defaultServerSideEncryption':
                        {
                            'isClientAuthorizedToRead': True,
                            'value': {
                                'algorithm': 'AES256',
                                'mode': 'SSE-B2'
                            }
                        },
                    'fileLockConfiguration':
                        {
                            'isClientAuthorizedToRead': True,
                            'value':
                                {
                                    'defaultRetention':
                                        {
                                            'mode': 'compliance',
                                            'period': {
                                                'unit': 'years',
                                                'duration': 7
                                            }
                                        },
                                    'isFileLockEnabled': None
                                }
                        },
                    'lifecycleRules': [{
                        'fileNamePrefix': 'is_life'
                    }],
                    'options': set(),
                    'revision': 2
                }, result
            )
        else:
            self.assertIsInstance(result, Bucket)
            assertions_mapping = {  # yapf: disable
                'id_': self.bucket.id_,
                'name': self.bucket.name,
                'type_': 'allPrivate',
                'bucket_info': {'info': 'o'},
                'cors_rules': {'andrea': 'corr'},
                'lifecycle_rules': [{'fileNamePrefix': 'is_life'}],
                'options_set': set(),
                'default_server_side_encryption': SSE_B2_AES,
                'default_retention': BucketRetentionSetting(RetentionMode.COMPLIANCE, RetentionPeriod(years=7)),
                'replication': REPLICATION,
            }
            for attr_name, attr_value in assertions_mapping.items():
                self.maxDiff = None
                print('---', attr_name, '---')
                print(attr_value)
                print('?=?')
                print(getattr(result, attr_name))
                self.assertEqual(attr_value, getattr(result, attr_name), attr_name)

    @pytest.mark.apiver(from_ver=2)
    def test_empty_replication(self):
        self.bucket.update(
            replication=ReplicationConfiguration(
                rules=[],
                source_to_destination_key_mapping={},
            ),
        )

    def test_update_if_revision_is(self):
        current_revision = self.bucket.revision
        self.bucket.update(
            lifecycle_rules=[{
                'fileNamePrefix': 'is_life'
            }],
            if_revision_is=current_revision,
        )
        updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        self.assertEqual([{'fileNamePrefix': 'is_life'}], updated_bucket.lifecycle_rules)

        try:
            self.bucket.update(
                lifecycle_rules=[{
                    'fileNamePrefix': 'is_life'
                }],
                if_revision_is=current_revision,  # this is now the old revision
            )
        except Exception:
            pass

        not_updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        self.assertEqual([{'fileNamePrefix': 'is_life'}], not_updated_bucket.lifecycle_rules)

    def test_is_file_lock_enabled(self):
        assert not self.bucket.is_file_lock_enabled

        # set is_file_lock_enabled to False when it's already false
        self.bucket.update(is_file_lock_enabled=False)
        updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        assert not updated_bucket.is_file_lock_enabled

        # sunny day scenario
        self.bucket.update(is_file_lock_enabled=True)
        updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        assert updated_bucket.is_file_lock_enabled
        assert self.simulator.bucket_name_to_bucket[self.bucket.name].is_file_lock_enabled

        # attempt to clear is_file_lock_enabled
        with pytest.raises(DisablingFileLockNotSupported):
            self.bucket.update(is_file_lock_enabled=False)
        updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        assert updated_bucket.is_file_lock_enabled

        # attempt to set is_file_lock_enabled when it's already set
        self.bucket.update(is_file_lock_enabled=True)
        updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        assert updated_bucket.is_file_lock_enabled

    @pytest.mark.apiver(from_ver=2)
    def test_is_file_lock_enabled_source_replication(self):
        assert not self.bucket.is_file_lock_enabled

        # attempt to set is_file_lock_enabled with source replication enabled
        self.bucket.update(replication=REPLICATION)
        with pytest.raises(SourceReplicationConflict):
            self.bucket.update(is_file_lock_enabled=True)
        updated_bucket = self.bucket.update(replication=REPLICATION)
        assert not updated_bucket.is_file_lock_enabled

        # sunny day scenario
        self.bucket.update(
            replication=ReplicationConfiguration(
                rules=[],
                source_to_destination_key_mapping={},
            )
        )
        self.bucket.update(is_file_lock_enabled=True)
        updated_bucket = self.api.get_bucket_by_name(self.bucket.name)
        assert updated_bucket.is_file_lock_enabled
        assert self.simulator.bucket_name_to_bucket[self.bucket.name].is_file_lock_enabled


class TestUpload(TestCaseWithBucket):
    def test_upload_bytes(self):
        data = b'hello world'
        file_info = self.bucket.upload_bytes(data, 'file1')
        self.assertTrue(isinstance(file_info, VFileVersionInfo))
        self._check_file_contents('file1', data)
        self._check_large_file_sha1('file1', None)
        self.assertEqual(file_info.server_side_encryption, SSE_NONE)

    def test_upload_bytes_file_retention(self):
        data = b'hello world'
        retention = FileRetentionSetting(RetentionMode.COMPLIANCE, 150)
        file_info = self.bucket.upload_bytes(
            data, 'file1', file_retention=retention, legal_hold=LegalHold.ON
        )
        self._check_file_contents('file1', data)
        self._check_large_file_sha1('file1', None)
        self.assertEqual(retention, file_info.file_retention)
        self.assertEqual(LegalHold.ON, file_info.legal_hold)

    def test_upload_bytes_sse_b2(self):
        data = b'hello world'
        file_info = self.bucket.upload_bytes(data, 'file1', encryption=SSE_B2_AES)
        self.assertTrue(isinstance(file_info, VFileVersionInfo))
        self.assertEqual(file_info.server_side_encryption, SSE_B2_AES)

    def test_upload_bytes_sse_c(self):
        data = b'hello world'
        file_info = self.bucket.upload_bytes(data, 'file1', encryption=SSE_C_AES)
        self.assertTrue(isinstance(file_info, VFileVersionInfo))
        self.assertEqual(SSE_C_AES_NO_SECRET, file_info.server_side_encryption)

    def test_upload_local_file_sse_b2(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = b'hello world'
            write_file(path, data)
            file_info = self.bucket.upload_local_file(path, 'file1', encryption=SSE_B2_AES)
            self.assertTrue(isinstance(file_info, VFileVersionInfo))
            self.assertEqual(file_info.server_side_encryption, SSE_B2_AES)
            self._check_file_contents('file1', data)

    def test_upload_local_file_sse_c(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = b'hello world'
            write_file(path, data)
            file_info = self.bucket.upload_local_file(path, 'file1', encryption=SSE_C_AES)
            self.assertTrue(isinstance(file_info, VFileVersionInfo))
            self.assertEqual(SSE_C_AES_NO_SECRET, file_info.server_side_encryption)
            self._check_file_contents('file1', data)

    def test_upload_local_file_retention(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = b'hello world'
            write_file(path, data)
            retention = FileRetentionSetting(RetentionMode.COMPLIANCE, 150)
            file_info = self.bucket.upload_local_file(
                path,
                'file1',
                encryption=SSE_C_AES,
                file_retention=retention,
                legal_hold=LegalHold.ON
            )
            self._check_file_contents('file1', data)
            self.assertEqual(retention, file_info.file_retention)
            self.assertEqual(LegalHold.ON, file_info.legal_hold)

    def test_upload_local_file_cache_control(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = b'hello world'
            write_file(path, data)
            file_info = self.bucket.upload_local_file(
                path, 'file1', encryption=SSE_C_AES, cache_control='max-age=3600'
            )
            self._check_file_contents('file1', data)
            self.assertEqual(file_info.cache_control, 'max-age=3600')

    def test_upload_bytes_cache_control(self):
        data = b'hello world'
        file_info = self.bucket.upload_bytes(
            data, 'file1', encryption=SSE_C_AES, cache_control='max-age=3600'
        )
        self.assertTrue(isinstance(file_info, VFileVersionInfo))
        self.assertEqual(file_info.cache_control, 'max-age=3600')

    def test_upload_bytes_progress(self):
        data = b'hello world'
        progress_listener = StubProgressListener()
        self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_local_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = b'hello world'
            write_file(path, data)
            file_info = self.bucket.upload_local_file(path, 'file1')
            self._check_file_contents('file1', data)
            self._check_large_file_sha1('file1', None)
            self.assertTrue(isinstance(file_info, VFileVersionInfo))
            self.assertEqual(file_info.server_side_encryption, SSE_NONE)
            print(file_info.as_dict())
            self.assertEqual(file_info.as_dict()['serverSideEncryption'], {'mode': 'none'})

    @pytest.mark.apiver(from_ver=2)
    def test_upload_local_file_incremental(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')

            small_data = b'Hello world!'
            big_data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
            DATA = [
                big_data,
                big_data + small_data,
                big_data + small_data + big_data,
                small_data,
                small_data + small_data,
                small_data.upper() + small_data,
            ]

            last_data = None
            for data in DATA:
                # figure out if this particular upload should be incremental
                should_be_incremental = (
                    last_data and data.startswith(last_data) and
                    len(last_data) >= self.simulator.MIN_PART_SIZE
                )

                # if it's incremental, then there should be two sources concatenated, otherwise one
                expected_source_count = 2 if should_be_incremental else 1

                # is the result file expected to be a large file
                expected_large_file = \
                    should_be_incremental or \
                    len(data) > self.simulator.MIN_PART_SIZE

                expected_parts_sizes = \
                    [len(last_data), len(data) - len(last_data)] \
                        if should_be_incremental else [len(data)]

                write_file(path, data)
                with mock.patch.object(
                    self.bucket, 'concatenate', wraps=self.bucket.concatenate
                ) as mocked_concatenate:
                    self.bucket.upload_local_file(path, 'file1', upload_mode=UploadMode.INCREMENTAL)
                    mocked_concatenate.assert_called_once()
                    call = mocked_concatenate.mock_calls[0]
                    # TODO: use .args[0] instead of [1][0] when we drop Python 3.7
                    assert len(call[1][0]) == expected_source_count
                    # Ensuring that the part sizes make sense.
                    parts_sizes = [entry.get_content_length() for entry in call[1][0]]
                    assert parts_sizes == expected_parts_sizes
                    if should_be_incremental:
                        # Ensuring that the first part is a copy.
                        # Order of indices: pick arguments, pick first argument, first element of the first argument.
                        self.assertIsInstance(call[1][0][0], CopySource)

                self._check_file_contents('file1', data)
                if expected_large_file:
                    self._check_large_file_sha1('file1', hex_sha1_of_bytes(data))

                last_data = data

    @pytest.mark.skipif(platform.system() == 'Windows', reason='no os.mkfifo() on Windows')
    def test_upload_fifo(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            os.mkfifo(path)
            with self.assertRaises(InvalidUploadSource):
                self.bucket.upload_local_file(path, 'file1')

    @pytest.mark.skipif(platform.system() == 'Windows', reason='no os.symlink() on Windows')
    def test_upload_dead_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            os.symlink('non-existing', path)
            with self.assertRaises(InvalidUploadSource):
                self.bucket.upload_local_file(path, 'file1')

    def test_upload_local_wrong_sha(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file123')
            data = b'hello world'
            write_file(path, data)
            with self.assertRaises(FileSha1Mismatch):
                self.bucket.upload_local_file(
                    path,
                    'file123',
                    sha1_sum='abcd' * 10,
                )

    def test_upload_one_retryable_error(self):
        self.simulator.set_upload_errors([CanRetry(True)])
        data = b'hello world'
        self.bucket.upload_bytes(data, 'file1')

    def test_upload_timeout(self):
        self.simulator.set_upload_errors([B2RequestTimeoutDuringUpload()])
        data = b'hello world'
        self.bucket.upload_bytes(data, 'file1')

    def test_upload_file_one_fatal_error(self):
        self.simulator.set_upload_errors([CanRetry(False)])
        data = b'hello world'
        with self.assertRaises(CanRetry):
            self.bucket.upload_bytes(data, 'file1')

    def test_upload_file_too_many_retryable_errors(self):
        self.simulator.set_upload_errors([CanRetry(True)] * 6)
        data = b'hello world'
        with self.assertRaises(MaxRetriesExceeded):
            self.bucket.upload_bytes(data, 'file1')

    def test_upload_large(self):
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        progress_listener = StubProgressListener()
        self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self._check_file_contents('file1', data)
        self._check_large_file_sha1('file1', hex_sha1_of_bytes(data))
        self.assertTrue(progress_listener.is_valid())

    def test_upload_local_large_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
            write_file(path, data)
            self.bucket.upload_local_file(path, 'file1')
            self._check_file_contents('file1', data)
            self._check_large_file_sha1('file1', hex_sha1_of_bytes(data))

    def test_upload_local_large_file_over_10k_parts(self):
        pytest.skip('this test is really slow and impedes development')  # TODO: fix it
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = self._make_data(self.simulator.MIN_PART_SIZE * 10001)  # 2MB on the simulator
            write_file(path, data)
            self.bucket.upload_local_file(path, 'file1')
            self._check_file_contents('file1', data)
            self._check_large_file_sha1('file1', hex_sha1_of_bytes(data))

    def test_create_file_over_10k_parts(self):
        data = b'hello world' * 20000

        f1_id = self.bucket.upload_bytes(data, 'f1').id_
        with tempfile.TemporaryDirectory():
            write_intents = [
                WriteIntent(
                    CopySource(f1_id, length=len(data), offset=0),
                    destination_offset=0,
                )
            ] * 10
            created_file = self.bucket.create_file(
                write_intents,
                file_name='created_file',
                min_part_size=10,
                max_part_size=200,
            )
            self.assertIsInstance(created_file, VFileVersionInfo)
            actual = (
                created_file.id_,
                created_file.file_name,
                created_file.size,
                created_file.server_side_encryption,
            )
            expected = ('9998', 'created_file', len(data), SSE_NONE)
            self.assertEqual(expected, actual)

    def test_upload_large_resume(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1')
        self._upload_part(large_file_id, 1, data[:part_size])
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self.assertEqual(large_file_id, file_info.id_)
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_resume_no_parts(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1')
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self.assertNotEqual(large_file_id, file_info.id_)  # it's not a match if there are no parts
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_resume_all_parts_there(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1')
        self._upload_part(large_file_id, 1, data[:part_size])
        self._upload_part(large_file_id, 2, data[part_size:2 * part_size])
        self._upload_part(large_file_id, 3, data[2 * part_size:])
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self.assertEqual(large_file_id, file_info.id_)
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_resume_part_does_not_match(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1')
        self._upload_part(large_file_id, 3, data[:part_size])  # wrong part number for this data
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self.assertNotEqual(large_file_id, file_info.id_)
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_resume_wrong_part_size(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1')
        self._upload_part(large_file_id, 1, data[:part_size + 1])  # one byte to much
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self.assertNotEqual(large_file_id, file_info.id_)
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_resume_file_info(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1', {'property': 'value1'})
        self._upload_part(large_file_id, 1, data[:part_size])
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(
            data, 'file1', progress_listener=progress_listener, file_info={'property': 'value1'}
        )
        self.assertEqual(large_file_id, file_info.id_)
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_resume_file_info_does_not_match(self):
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        large_file_id = self._start_large_file('file1', {'property': 'value1'})
        self._upload_part(large_file_id, 1, data[:part_size])
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(
            data, 'file1', progress_listener=progress_listener, file_info={'property': 'value2'}
        )
        self.assertNotEqual(large_file_id, file_info.id_)
        self._check_file_contents('file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_large_file_with_restricted_api_key(self):
        self.simulator.key_id_to_key[self.account_id].name_prefix_or_none = 'path/to'
        part_size = self.simulator.MIN_PART_SIZE
        data = self._make_data(part_size * 3)
        progress_listener = StubProgressListener()
        file_info = self.bucket.upload_bytes(
            data, 'path/to/file1', progress_listener=progress_listener
        )
        self.assertEqual(len(data), file_info.size)
        self._check_file_contents('path/to/file1', data)
        self.assertTrue(progress_listener.is_valid())

    def test_upload_stream(self):
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        self.bucket.upload_unbound_stream(io.BytesIO(data), 'file1')
        self._check_file_contents('file1', data)

    def test_upload_stream_from_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
            write_file(path, data)
            with open(path, 'rb') as f:
                self.bucket.upload_unbound_stream(f, 'file1')
            self._check_file_contents('file1', data)

    def _start_large_file(self, file_name, file_info=None):
        if file_info is None:
            file_info = {}
        large_file_info = self.simulator.start_large_file(
            self.api_url, self.account_auth_token, self.bucket_id, file_name, None, file_info
        )
        return large_file_info['fileId']

    def _upload_part(self, large_file_id, part_number, part_data):
        part_stream = BytesIO(part_data)
        upload_info = self.simulator.get_upload_part_url(
            self.api_url, self.account_auth_token, large_file_id
        )
        self.simulator.upload_part(
            upload_info['uploadUrl'], upload_info['authorizationToken'], part_number,
            len(part_data), hex_sha1_of_bytes(part_data), part_stream
        )


class TestBucketRaisingSession(TestUpload):
    def get_api(self):
        class B2SessionRaising(B2Session):
            def __init__(self, *args, **kwargs):
                self._raise_count = 0
                self._raise_until = 1
                super().__init__(*args, **kwargs)

            def upload_part(
                self,
                file_id,
                part_number,
                content_length,
                sha1_sum,
                input_stream,
                server_side_encryption=None
            ):
                if self._raise_count < self._raise_until:
                    self._raise_count += 1
                    raise B2ConnectionError()
                return super().upload_part(
                    file_id, part_number, content_length, sha1_sum, input_stream,
                    server_side_encryption
                )

        class B2ApiPatched(B2Api):
            SESSION_CLASS = staticmethod(B2SessionRaising)

        self.api = B2ApiPatched(
            self.account_info,
            cache=self.CACHE_CLASS(),
            api_config=B2HttpApiConfig(_raw_api_class=self.RAW_SIMULATOR_CLASS),
        )
        return self.api

    def test_upload_chunk_retry_stream_open(self):
        assert self.api.session._raise_count == 0
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        self.bucket.upload_unbound_stream(io.BytesIO(data), 'file1')
        self._check_file_contents('file1', data)
        assert self.api.session._raise_count == 1

    def test_upload_chunk_stream_guard_closes(self):
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        large_file_upload_state = mock.MagicMock()
        large_file_upload_state.has_error.return_value = False

        class TrackedUploadSourceBytes(UploadSourceBytes):
            def __init__(self, *args, **kwargs):
                self._close_called = 0
                super().__init__(*args, **kwargs)

            def open(self):
                class TrackedBytesIO(io.BytesIO):
                    def __init__(self, parent, *args, **kwargs):
                        self._parent = parent
                        super().__init__(*args, **kwargs)

                    def close(self):
                        self._parent._close_called += 1
                        return super().close()

                return TrackedBytesIO(self, self.data_bytes)

        data_source = TrackedUploadSourceBytes(data)
        assert data_source._close_called == 0
        file_id = self._start_large_file('file1')
        self.api.services.upload_manager.upload_part(
            self.bucket_id, file_id, data_source, 1, large_file_upload_state
        ).result()
        # one retry means two potential callback calls, but we want one only
        assert data_source._close_called == 1


class TestConcatenate(TestCaseWithBucket):
    def _create_remote(self, sources, file_name, encryption=None):
        return self.bucket.concatenate(sources, file_name=file_name, encryption=encryption)

    def test_create_remote(self):
        data = b'hello world'

        f1_id = self.bucket.upload_bytes(data, 'f1').id_
        f2_id = self.bucket.upload_bytes(data, 'f1').id_
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file')
            write_file(path, data)
            created_file = self._create_remote(
                [
                    CopySource(f1_id, length=len(data), offset=0),
                    UploadSourceLocalFile(path),
                    CopySource(f2_id, length=len(data), offset=0),
                ],
                file_name='created_file'
            )
            self.assertIsInstance(created_file, VFileVersionInfo)
            actual = (
                created_file.id_, created_file.file_name, created_file.size,
                created_file.server_side_encryption
            )
            expected = ('9997', 'created_file', 33, SSE_NONE)
            self.assertEqual(expected, actual)

    def test_create_remote_encryption(self):
        for data in [b'hello_world', self._make_data(self.simulator.MIN_PART_SIZE * 3)]:
            f1_id = self.bucket.upload_bytes(data, 'f1', encryption=SSE_C_AES).id_
            f2_id = self.bucket.upload_bytes(data, 'f1', encryption=SSE_C_AES_2).id_
            with tempfile.TemporaryDirectory() as d:
                path = os.path.join(d, 'file')
                write_file(path, data)
                created_file = self._create_remote(
                    [
                        CopySource(f1_id, length=len(data), offset=0, encryption=SSE_C_AES),
                        UploadSourceLocalFile(path),
                        CopySource(f2_id, length=len(data), offset=0, encryption=SSE_C_AES_2),
                    ],
                    file_name=f'created_file_{len(data)}',
                    encryption=SSE_C_AES
                )
            self.assertIsInstance(created_file, VFileVersionInfo)
            actual = (
                created_file.id_, created_file.file_name, created_file.size,
                created_file.server_side_encryption
            )
            expected = (
                mock.ANY,
                f'created_file_{len(data)}',
                mock.ANY,  # FIXME: this should be equal to len(data) * 3,
                # but there is a problem in the simulator/test code somewhere
                SSE_C_AES_NO_SECRET
            )
            self.assertEqual(expected, actual)


class TestCreateFile(TestConcatenate):
    def _create_remote(self, sources, file_name, encryption=None):
        return self.bucket.create_file(
            [wi for wi in WriteIntent.wrap_sources_iterator(sources)],
            file_name=file_name,
            encryption=encryption
        )


class TestConcatenateStream(TestConcatenate):
    def _create_remote(self, sources, file_name, encryption=None):
        return self.bucket.concatenate_stream(sources, file_name=file_name, encryption=encryption)


class TestCreateFileStream(TestConcatenate):
    def _create_remote(self, sources, file_name, encryption=None):
        return self.bucket.create_file_stream(
            [wi for wi in WriteIntent.wrap_sources_iterator(sources)],
            file_name=file_name,
            encryption=encryption
        )


class TestCustomTimestamp(TestCaseWithBucket):
    def test_custom_timestamp(self):
        data = b'hello world'

        # upload
        self.bucket.upload_bytes(data, 'file0', custom_upload_timestamp=0)

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            write_file(path, data)
            self.bucket.upload_local_file(path, 'file1', custom_upload_timestamp=1)

        upload_source = UploadSourceBytes(data)
        self.bucket.upload(upload_source, 'file2', custom_upload_timestamp=2)
        self.bucket.upload_unbound_stream(io.BytesIO(data), 'file3', custom_upload_timestamp=3)

        # concatenate
        self.bucket.concatenate([upload_source], 'file4', custom_upload_timestamp=4)
        self.bucket.concatenate_stream([upload_source], 'file5', custom_upload_timestamp=5)

        # create_file
        self.bucket.create_file(
            [WriteIntent(upload_source, destination_offset=0)], 'file6', custom_upload_timestamp=6
        )
        self.bucket.create_file_stream(
            [WriteIntent(upload_source, destination_offset=0)], 'file7', custom_upload_timestamp=7
        )

        def ls(bucket):
            return [(info.file_name, info.upload_timestamp) for (info, folder) in bucket_ls(bucket)]

        expected = [
            ('file0', 0),
            ('file1', 1),
            ('file2', 2),
            ('file3', 3),
            ('file4', 4),
            ('file5', 5),
            ('file6', 6),
            ('file7', 7),
        ]
        self.assertEqual(ls(self.bucket), expected)


class DownloadTestsBase:
    DATA = NotImplemented

    def setUp(self):
        super().setUp()
        self.file_version = self.bucket.upload_bytes(self.DATA.encode(), 'file1')
        self.encrypted_file_version = self.bucket.upload_bytes(
            self.DATA.encode(), 'enc_file1', encryption=SSE_C_AES
        )
        self.bytes_io = io.BytesIO()
        if apiver_deps.V <= 1:
            self.download_dest = DownloadDestBytes()
        else:
            self.download_dest = None
        self.progress_listener = StubProgressListener()

    def _verify(self, expected_result, check_progress_listener=True):
        self._assert_downloaded_data(expected_result)
        if check_progress_listener:
            valid, reason = self.progress_listener.is_valid_reason(
                check_progress=False,
                check_monotonic_progress=True,
            )
            assert valid, reason

    def _assert_downloaded_data(self, expected_result):
        if apiver_deps.V <= 1:
            assert self.download_dest.get_bytes_written() == expected_result.encode()
        else:
            assert self.bytes_io.getvalue() == expected_result.encode()

    def download_file_by_id(self, file_id, v1_download_dest=None, v2_file=None, **kwargs):
        if apiver_deps.V <= 1:
            self.bucket.download_file_by_id(
                file_id, v1_download_dest or self.download_dest, **kwargs
            )
        else:
            self.bucket.download_file_by_id(file_id, **kwargs).save(v2_file or self.bytes_io)

    def download_file_by_name(self, file_name, download_dest=None, **kwargs):
        if apiver_deps.V <= 1:
            self.bucket.download_file_by_name(
                file_name, download_dest or self.download_dest, **kwargs
            )
        else:
            self.bucket.download_file_by_name(file_name, **kwargs).save(self.bytes_io)


class TestDownloadException(DownloadTestsBase, TestCaseWithBucket):
    DATA = 'some data'

    def test_download_file_by_name(self):
        if apiver_deps.V <= 1:
            exception_class = AssertionError
        else:
            exception_class = ValueError
        with mock.patch.object(self.bucket.api.services.download_manager, 'strategies', new=[]):
            with pytest.raises(exception_class) as exc_info:
                self.download_file_by_name(self.file_version.file_name)
            assert str(exc_info.value) == 'no strategy suitable for download was found!'


class DownloadTests(DownloadTestsBase):
    DATA = 'abcdefghijklmnopqrs'

    def test_v2_return_types(self):
        download_kwargs = {
            'range_': (7, 18),
            'encryption': SSE_C_AES,
            'progress_listener': self.progress_listener,
        }
        file_version = self.bucket.upload_bytes(
            self.DATA.encode(), 'enc_file2', encryption=SSE_C_AES
        )
        other_properties = {
            'download_version':
                DownloadVersion(
                    api=self.api,
                    id_=file_version.id_,
                    file_name=file_version.file_name,
                    size=len(self.DATA),
                    content_type=file_version.content_type,
                    content_sha1=file_version.content_sha1,
                    file_info=file_version.file_info,
                    upload_timestamp=file_version.upload_timestamp,
                    server_side_encryption=file_version.server_side_encryption,
                    range_=Range(7, 18),
                    content_disposition=None,
                    content_length=12,
                    content_language=None,
                    expires=None,
                    cache_control=None,
                    content_encoding=None,
                    file_retention=file_version.file_retention,
                    legal_hold=file_version.legal_hold,
                ),
        }
        ret = self.bucket.download_file_by_id(file_version.id_, **download_kwargs)
        assert isinstance(ret, DownloadedFile), type(ret)
        for attr_name, expected_value in {**download_kwargs, **other_properties}.items():
            assert getattr(ret, attr_name) == expected_value, attr_name

        if apiver_deps.V >= 2:
            ret = self.bucket.download_file_by_name(file_version.file_name, **download_kwargs)
            assert isinstance(ret, DownloadedFile), type(ret)
            for attr_name, expected_value in {**download_kwargs, **other_properties}.items():
                assert getattr(ret, attr_name) == expected_value, attr_name

        ret = file_version.download(**download_kwargs)
        assert isinstance(ret, DownloadedFile), type(ret)
        for attr_name, expected_value in {**download_kwargs, **other_properties}.items():
            assert getattr(ret, attr_name) == expected_value, attr_name

    @pytest.mark.apiver(to_ver=1)
    def test_v1_return_types(self):
        expected = {
            'contentLength': 19,
            'contentSha1': '893e69ff0109f3459c4243013b3de8b12b41a30e',
            'contentType': 'b2/x-auto',
            'fileId': '9999',
            'fileInfo': {},
            'fileName': 'file1'
        }
        ret = self.bucket.download_file_by_id(self.file_version.id_, self.download_dest)
        assert ret == expected
        ret = self.bucket.download_file_by_name(self.file_version.file_name, self.download_dest)
        assert ret == expected

    def test_download_file_version(self):
        self.file_version.download().save(self.bytes_io)
        assert self.bytes_io.getvalue() == self.DATA.encode()
        # self._verify performs different checks based on apiver,
        # but this is a new feature so it works the same on v2, v1 and v0

    def test_download_by_id_no_progress(self):
        self.download_file_by_id(self.file_version.id_)
        self._verify(self.DATA, check_progress_listener=False)

    def test_download_by_name_no_progress(self):
        self.download_file_by_name('file1')
        self._verify(self.DATA, check_progress_listener=False)

    def test_download_by_name_progress(self):
        self.download_file_by_name('file1', progress_listener=self.progress_listener)
        self._verify(self.DATA)

    def test_download_by_id_progress(self):
        self.download_file_by_id(self.file_version.id_, progress_listener=self.progress_listener)
        self._verify(self.DATA)

    def test_download_by_id_progress_partial(self):
        self.download_file_by_id(
            self.file_version.id_, progress_listener=self.progress_listener, range_=(3, 9)
        )
        self._verify('defghij')

    def test_download_by_id_progress_exact_range(self):
        self.download_file_by_id(
            self.file_version.id_, progress_listener=self.progress_listener, range_=(0, 18)
        )
        self._verify(self.DATA)

    def test_download_by_id_progress_range_one_off(self):
        with self.assertRaises(
            InvalidRange,
            msg='A range of 0-19 was requested (size of 20), but cloud could only serve 19 of that',
        ):
            self.download_file_by_id(
                self.file_version.id_,
                progress_listener=self.progress_listener,
                range_=(0, 19),
            )

    @pytest.mark.apiver(to_ver=1)
    def test_download_by_id_progress_partial_inplace_overwrite_v1(self):
        # LOCAL is
        # 12345678901234567890
        #
        # and then:
        #
        # abcdefghijklmnopqrs
        #    |||||||
        #    |||||||
        #    vvvvvvv
        #
        # 123defghij1234567890

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file2')
            download_dest = PreSeekedDownloadDest(seek_target=3, local_file_path=path)
            data = b'12345678901234567890'
            write_file(path, data)
            self.download_file_by_id(
                self.file_version.id_,
                download_dest,
                progress_listener=self.progress_listener,
                range_=(3, 9),
            )
            self._check_local_file_contents(path, b'123defghij1234567890')

    @pytest.mark.apiver(from_ver=2)
    def test_download_by_id_progress_partial_inplace_overwrite_v2(self):
        # LOCAL is
        # 12345678901234567890
        #
        # and then:
        #
        # abcdefghijklmnopqrs
        #    |||||||
        #    |||||||
        #    vvvvvvv
        #
        # 123defghij1234567890

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file2')
            data = b'12345678901234567890'
            write_file(path, data)
            with open(path, 'rb+') as file:
                file.seek(3)
                self.download_file_by_id(
                    self.file_version.id_,
                    v2_file=file,
                    progress_listener=self.progress_listener,
                    range_=(3, 9),
                )
            self._check_local_file_contents(path, b'123defghij1234567890')

    @pytest.mark.apiver(from_ver=2)
    def test_download_update_mtime_v2(self):
        with tempfile.TemporaryDirectory() as d:
            file_version = self.bucket.upload_bytes(
                self.DATA.encode(), 'file1', file_info={'src_last_modified_millis': '1000'}
            )
            path = os.path.join(d, 'file2')
            self.bucket.download_file_by_id(file_version.id_).save_to(path)
            assert pytest.approx(1, rel=0.001) == os.path.getmtime(path)

    @pytest.mark.apiver(to_ver=1)
    def test_download_by_id_progress_partial_shifted_overwrite_v1(self):
        # LOCAL is
        # 12345678901234567890
        #
        # and then:
        #
        # abcdefghijklmnopqrs
        #    |||||||
        #    \\\\\\\
        #     \\\\\\\
        #      \\\\\\\
        #       \\\\\\\
        #        \\\\\\\
        #        |||||||
        #        vvvvvvv
        #
        # 1234567defghij567890

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file2')
            download_dest = PreSeekedDownloadDest(seek_target=7, local_file_path=path)
            data = b'12345678901234567890'
            write_file(path, data)
            self.download_file_by_id(
                self.file_version.id_,
                download_dest,
                progress_listener=self.progress_listener,
                range_=(3, 9),
            )
            self._check_local_file_contents(path, b'1234567defghij567890')

    @pytest.mark.apiver(from_ver=2)
    def test_download_by_id_progress_partial_shifted_overwrite_v2(self):
        # LOCAL is
        # 12345678901234567890
        #
        # and then:
        #
        # abcdefghijklmnopqrs
        #    |||||||
        #    \\\\\\\
        #     \\\\\\\
        #      \\\\\\\
        #       \\\\\\\
        #        \\\\\\\
        #        |||||||
        #        vvvvvvv
        #
        # 1234567defghij567890

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file2')
            data = b'12345678901234567890'
            write_file(path, data)
            with open(path, 'rb+') as file:
                file.seek(7)
                self.download_file_by_id(
                    self.file_version.id_,
                    v2_file=file,
                    progress_listener=self.progress_listener,
                    range_=(3, 9),
                )
            self._check_local_file_contents(path, b'1234567defghij567890')

    def test_download_by_id_no_progress_encryption(self):
        self.download_file_by_id(self.encrypted_file_version.id_, encryption=SSE_C_AES)
        self._verify(self.DATA, check_progress_listener=False)

    def test_download_by_id_no_progress_wrong_encryption(self):
        with self.assertRaises(SSECKeyError):
            self.download_file_by_id(self.encrypted_file_version.id_, encryption=SSE_C_AES_2)

    def _check_local_file_contents(self, path, expected_contents):
        with open(path, 'rb') as f:
            contents = f.read()
            self.assertEqual(contents, expected_contents)

    @pytest.mark.apiver(from_ver=2)
    def test_download_to_non_seekable_file(self):
        file_version = self.bucket.upload_bytes(self.DATA.encode(), 'file1')

        non_seekable_strategies = [
            strat for strat in self.bucket.api.services.download_manager.strategies
            if not isinstance(strat, ParallelDownloader)
        ]
        context = contextlib.nullcontext() if non_seekable_strategies else pytest.raises(
            ValueError,
            match='no strategy suitable for download was found!',
        )
        output_file = NonSeekableIO()
        with context:
            self.download_file_by_id(
                file_version.id_,
                v2_file=output_file,
            )
            assert output_file.getvalue() == self.DATA.encode()


# download empty file


class EmptyFileDownloadScenarioMixin:
    """ use with DownloadTests, but not for TestDownloadParallel as it does not like empty files """

    def test_download_by_name_empty_file(self):
        self.file_version = self.bucket.upload_bytes(b'', 'empty')
        self.download_file_by_name('empty', progress_listener=self.progress_listener)
        self._verify('')


class UnverifiedChecksumDownloadScenarioMixin:
    """ use with DownloadTests """

    def test_download_by_name_unverified_checksum(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'file1')
            data = b'hello world'
            write_file(path, data)
            file_info = self.bucket.upload_local_file(path, 'file1')
            simulated_file = list(self.simulator.bucket_name_to_bucket.values()
                                 )[0].file_id_to_file[file_info.id_]
            simulated_file.content_sha1 = 'unverified:' + simulated_file.content_sha1
            #, sha1_sum='unverified:2aae6c35c94fcfb415dbe95f408b9ce91ee846ed')

            self.download_file_by_name('file1', progress_listener=self.progress_listener)

            self._verify('hello world')


# actual tests

# test choosing strategy


@pytest.mark.apiver(from_ver=2)
class TestChooseStrategy(TestCaseWithBucket):
    def test_choose_strategy(self):
        file_version = self.bucket.upload_bytes(b'hello world' * 8, 'file1')
        download_manager = self.bucket.api.services.download_manager
        parallel_downloader = ParallelDownloader(
            force_chunk_size=1,
            max_streams=32,
            min_part_size=16,
            thread_pool=download_manager._thread_pool,
        )
        simple_downloader = download_manager.strategies[1]
        download_manager.strategies = [
            parallel_downloader,
            simple_downloader,
        ]
        with io.BytesIO() as bytes_io:
            downloaded_file = self.bucket.download_file_by_id(file_version.id_)
            downloaded_file.save(bytes_io, allow_seeking=True)
            assert downloaded_file.download_strategy == parallel_downloader

            downloaded_file = self.bucket.download_file_by_id(file_version.id_)
            downloaded_file.save(bytes_io, allow_seeking=False)
            assert downloaded_file.download_strategy == simple_downloader

            downloaded_file = self.bucket.download_file_by_name(file_version.file_name)
            downloaded_file.save(bytes_io, allow_seeking=True)
            assert downloaded_file.download_strategy == parallel_downloader

            downloaded_file = self.bucket.download_file_by_name(file_version.file_name)
            downloaded_file.save(bytes_io, allow_seeking=False)
            assert downloaded_file.download_strategy == simple_downloader


# Default tests


class TestDownloadDefault(DownloadTests, EmptyFileDownloadScenarioMixin, TestCaseWithBucket):
    pass


class TestDownloadSimple(
    DownloadTests,
    UnverifiedChecksumDownloadScenarioMixin,
    EmptyFileDownloadScenarioMixin,
    TestCaseWithBucket,
):
    def setUp(self):
        super().setUp()
        download_manager = self.bucket.api.services.download_manager
        download_manager.strategies = [SimpleDownloader(force_chunk_size=20)]


class TestDownloadParallel(
    DownloadTests,
    UnverifiedChecksumDownloadScenarioMixin,
    TestCaseWithBucket,
):
    def setUp(self):
        super().setUp()
        download_manager = self.bucket.api.services.download_manager
        download_manager.strategies = [
            ParallelDownloader(
                force_chunk_size=2,
                max_streams=999,
                min_part_size=2,
            ),
        ]


class TestDownloadParallelALotOfStreams(DownloadTestsBase, TestCaseWithBucket):
    DATA = ''.join(['01234567890abcdef'] * 32)

    def setUp(self):
        super().setUp()
        download_manager = self.bucket.api.services.download_manager
        download_manager.strategies = [
            # this should produce 32 streams with 16 single byte writes
            # so we increase probability of non-sequential writes as much as possible
            # with great help from random sleeps in FakeResponse
            ParallelDownloader(
                force_chunk_size=1,
                max_streams=32,
                min_part_size=16,
                thread_pool=download_manager._thread_pool,
            ),
        ]

    def test_download_by_id_progress_monotonic(self):
        self.download_file_by_id(self.file_version.id_, progress_listener=self.progress_listener)
        self._verify(self.DATA)


# Truncated downloads


class TruncatedFakeResponse(FakeResponse):
    """
    A special FakeResponse class which returns only the first 4 bytes of data.
    Use it to test followup retries for truncated download issues.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_bytes = self.data_bytes[:4]


class TruncatedDownloadBucketSimulator(BucketSimulator):
    RESPONSE_CLASS = TruncatedFakeResponse


class TruncatedDownloadRawSimulator(RawSimulator):
    BUCKET_SIMULATOR_CLASS = TruncatedDownloadBucketSimulator


class TestCaseWithTruncatedDownloadBucket(TestCaseWithBucket):
    RAW_SIMULATOR_CLASS = TruncatedDownloadRawSimulator


####### actual tests of truncated downloads


class TestTruncatedDownloadSimple(DownloadTests, TestCaseWithTruncatedDownloadBucket):
    def setUp(self):
        super().setUp()
        download_manager = self.bucket.api.services.download_manager
        download_manager.strategies = [SimpleDownloader(force_chunk_size=20)]


class TestTruncatedDownloadParallel(DownloadTests, TestCaseWithTruncatedDownloadBucket):
    def setUp(self):
        super().setUp()
        download_manager = self.bucket.api.services.download_manager
        download_manager.strategies = [
            ParallelDownloader(
                force_chunk_size=3,
                max_streams=2,
                min_part_size=2,
            )
        ]


class DummyDownloader(AbstractDownloader):
    def download(self, *args, **kwargs):
        pass


@pytest.mark.parametrize(
    'min_chunk_size,max_chunk_size,content_length,align_factor,expected_chunk_size',
    [
        (10, 100, 1000 * 9, 8, 8),  # min_chunk_size aligned
        (10, 100, 1000 * 17, 8, 16),  # content_length // 1000 aligned
        (10, 100, 1000 * 108, 8, 96),  # max_chunk_size // 1000 aligned
        (10, 100, 1000 * 9, 100, 100),  # max_chunk_size/align_factor
        (10, 100, 1000 * 17, 100, 100),  # max_chunk_size/align_factor
        (10, 100, 1000 * 108, 100, 100),  # max_chunk_size/align_factor
        (10, 100, 1, 100, 100),  # max_chunk_size/align_factor
    ],
)
def test_downloader_get_chunk_size(
    min_chunk_size, max_chunk_size, content_length, align_factor, expected_chunk_size
):
    downloader = DummyDownloader(
        min_chunk_size=min_chunk_size,
        max_chunk_size=max_chunk_size,
        align_factor=align_factor,
    )
    assert downloader._get_chunk_size(content_length) == expected_chunk_size


@pytest.mark.apiver(from_ver=2)
class TestDownloadTuneWriteBuffer(DownloadTestsBase, TestCaseWithBucket):
    ALIGN_FACTOR = 123
    DATA = 'abc' * 4096

    def get_api(self):
        return B2Api(
            self.account_info,
            api_config=B2HttpApiConfig(_raw_api_class=self.RAW_SIMULATOR_CLASS),
            save_to_buffer_size=self.ALIGN_FACTOR,
        )

    def test_get_chunk_size_alignment(self):
        download_manager = self.bucket.api.services.download_manager
        for downloader in download_manager.strategies:
            assert downloader._get_chunk_size(len(self.DATA)) % self.ALIGN_FACTOR == 0

    def test_buffering_in_save_to(self):
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / 'file2'
            with mock.patch('b2sdk.transfer.inbound.downloaded_file.open') as mock_open:
                mock_open.side_effect = open
                self.bucket.download_file_by_id(self.file_version.id_).save_to(path)
                mock_open.assert_called_once_with(path, mock.ANY, buffering=self.ALIGN_FACTOR)
                assert path.read_text() == self.DATA

    def test_set_write_buffer_parallel_called_get_chunk_size(self):
        self._check_called_on_downloader(
            ParallelDownloader(
                force_chunk_size=len(self.DATA) // 3,
                max_streams=999,
                min_part_size=len(self.DATA) // 3,
            )
        )

    def test_set_write_buffer_simple_called_get_chunk_size(self):
        self._check_called_on_downloader(SimpleDownloader(force_chunk_size=len(self.DATA) // 3))

    def _check_called_on_downloader(self, downloader):
        download_manager = self.bucket.api.services.download_manager
        download_manager.strategies = [downloader]
        orig_get_chunk_size = downloader._get_chunk_size
        with mock.patch.object(downloader, '_get_chunk_size') as mock_get_chunk_size:
            mock_get_chunk_size.side_effect = orig_get_chunk_size
            self.download_file_by_id(self.file_version.id_)
            self._verify(self.DATA, check_progress_listener=False)
            assert mock_get_chunk_size.called


@pytest.mark.apiver(from_ver=2)
class TestDownloadNoHashChecking(DownloadTestsBase, TestCaseWithBucket):
    DATA = 'abcdefghijklmnopqrs'

    def get_api(self):
        return B2Api(
            self.account_info,
            api_config=B2HttpApiConfig(_raw_api_class=self.RAW_SIMULATOR_CLASS),
            check_download_hash=False,
        )

    def test_download_by_id_no_hash_checking(self):
        downloaded_file = self.bucket.download_file_by_id(self.file_version.id_)
        orig_validate_download = downloaded_file._validate_download
        with mock.patch.object(downloaded_file, '_validate_download') as mocked_validate_download:
            mocked_validate_download.side_effect = orig_validate_download
            downloaded_file.save(self.bytes_io)
            self._verify(self.DATA, check_progress_listener=False)
            mocked_validate_download.assert_called_once_with(mock.ANY, '')
            assert downloaded_file.download_version.content_sha1 != 'none'
            assert downloaded_file.download_version.content_sha1 != ''


class DecodeTestsBase:
    def setUp(self):
        super().setUp()
        self.bucket.upload_bytes(
            b'Test File 1', 'test.txt?foo=bar', file_info={'custom_info': 'aaa?bbb'}
        )
        self.bucket.upload_bytes(
            b'Test File 2', 'test.txt%3Ffoo=bar', file_info={'custom_info': 'aaa%3Fbbb'}
        )
        self.bucket.upload_bytes(b'Test File 3', 'test.txt%3Ffoo%3Dbar')
        self.bucket.upload_bytes(b'Test File 4', 'test.txt%253Ffoo%253Dbar')
        self.bytes_io = io.BytesIO()
        if apiver_deps.V <= 1:
            self.download_dest = DownloadDestBytes()
        else:
            self.download_dest = None
        self.progress_listener = StubProgressListener()

    def _verify(self, expected_result, check_progress_listener=True):
        self._assert_downloaded_data(expected_result)
        if check_progress_listener:
            valid, reason = self.progress_listener.is_valid_reason(
                check_progress=False,
                check_monotonic_progress=True,
            )
            assert valid, reason

    def _assert_downloaded_data(self, expected_result):
        if apiver_deps.V <= 1:
            assert self.download_dest.get_bytes_written() == expected_result.encode()
        else:
            assert self.bytes_io.getvalue() == expected_result.encode()

    def download_file_by_name(self, file_name, download_dest=None, **kwargs):
        if apiver_deps.V <= 1:
            self.bucket.download_file_by_name(
                file_name, download_dest or self.download_dest, **kwargs
            )
        else:
            self.bucket.download_file_by_name(file_name, **kwargs).save(self.bytes_io)


class DecodeTests(DecodeTestsBase, TestCaseWithBucket):
    def test_file_content_1(self):
        self.download_file_by_name('test.txt?foo=bar', progress_listener=self.progress_listener)
        self._verify("Test File 1")

    def test_file_content_2(self):
        self.download_file_by_name('test.txt%3Ffoo=bar', progress_listener=self.progress_listener)
        self._verify("Test File 2")

    def test_file_content_3(self):
        self.download_file_by_name('test.txt%3Ffoo%3Dbar', progress_listener=self.progress_listener)
        self._verify("Test File 3")

    def test_file_content_4(self):
        self.download_file_by_name(
            'test.txt%253Ffoo%253Dbar', progress_listener=self.progress_listener
        )
        self._verify("Test File 4")

    def test_file_info_1(self):
        download_version = self.bucket.get_file_info_by_name('test.txt?foo=bar')
        assert download_version.file_name == 'test.txt?foo=bar'
        assert download_version.file_info['custom_info'] == 'aaa?bbb'

    def test_file_info_2(self):
        download_version = self.bucket.get_file_info_by_name('test.txt%3Ffoo=bar')
        assert download_version.file_name == 'test.txt%3Ffoo=bar'
        assert download_version.file_info['custom_info'] == 'aaa%3Fbbb'

    def test_file_info_3(self):
        download_version = self.bucket.get_file_info_by_name('test.txt%3Ffoo%3Dbar')
        assert download_version.file_name == 'test.txt%3Ffoo%3Dbar'

    def test_file_info_4(self):
        download_version = self.bucket.get_file_info_by_name('test.txt%253Ffoo%253Dbar')
        assert download_version.file_name == 'test.txt%253Ffoo%253Dbar'


class TestAuthorizeForBucket(TestCaseWithBucket):
    CACHE_CLASS = InMemoryCache

    @pytest.mark.apiver(from_ver=2)
    def test_authorize_for_bucket_ensures_cache(self):
        key = create_key(
            self.api,
            key_name='singlebucket',
            capabilities=[
                'listBuckets',
            ],
            bucket_id=self.bucket_id,
        )

        self.api.authorize_account(
            application_key_id=key.id_,
            application_key=key.application_key,
            realm='production',
        )

        # Check whether the bucket fetching performs an API call.
        with mock.patch.object(self.api, 'list_buckets') as mock_list_buckets:
            self.api.get_bucket_by_id(self.bucket_id)
            mock_list_buckets.assert_not_called()

            self.api.get_bucket_by_name(self.bucket_name)
            mock_list_buckets.assert_not_called()

    @pytest.mark.apiver(from_ver=2)
    def test_authorize_for_non_existing_bucket(self):
        key = create_key(
            self.api,
            key_name='singlebucket',
            capabilities=[
                'listBuckets',
            ],
            bucket_id=self.bucket_id + 'x',
        )

        with self.assertRaises(RestrictedBucketMissing):
            self.api.authorize_account(
                application_key_id=key.id_,
                application_key=key.application_key,
                realm='production',
            )


class TestDownloadLocalDirectoryIssues(TestCaseWithBucket):
    def setUp(self):
        super().setUp()
        self.file_version = self.bucket.upload_bytes(b'test-data', 'file1')
        self.bytes_io = io.BytesIO()
        self.progress_listener = StubProgressListener()

    @pytest.mark.apiver(from_ver=2)
    def test_download_file_to_unknown_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = pathlib.Path(temp_dir) / 'non-existing-directory' / 'some-file'
            with self.assertRaises(DestinationDirectoryDoesntExist):
                self.bucket.download_file_by_name(self.file_version.file_name).save_to(target_file)

    @pytest.mark.apiver(from_ver=2)
    def test_download_file_targeting_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = pathlib.Path(temp_dir) / 'existing-directory'
            os.makedirs(target_file, exist_ok=True)

            with self.assertRaises(DestinationIsADirectory):
                self.bucket.download_file_by_name(self.file_version.file_name).save_to(target_file)

    @pytest.mark.apiver(from_ver=2)
    def test_download_file_targeting_directory_is_a_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            some_file = pathlib.Path(temp_dir) / 'existing-file'
            some_file.write_bytes(b'i-am-a-file')
            target_file = some_file / 'save-target'

            with self.assertRaises(DestinationParentIsNotADirectory):
                self.bucket.download_file_by_name(self.file_version.file_name).save_to(target_file)

    @pytest.mark.apiver(from_ver=2)
    @pytest.mark.skipif(
        platform.system() == 'Windows',
        reason='os.chmod on Windows only affects read-only flag for files',
    )
    def test_download_file_no_access_to_directory(self):
        chain = contextlib.ExitStack()
        temp_dir = chain.enter_context(tempfile.TemporaryDirectory())

        with chain:
            target_directory = pathlib.Path(temp_dir) / 'impossible-directory'

            os.makedirs(target_directory, exist_ok=True)
            # Don't allow any operation on this directory. Used explicitly, as the documentation
            # states that on some platforms passing mode to `makedirs` may be ignored.
            os.chmod(target_directory, mode=0)

            # Ensuring that whenever we exit this context, our directory will be removable.
            chain.push(lambda *args, **kwargs: os.chmod(target_directory, mode=0o777))

            target_file = target_directory / 'target_file'
            with self.assertRaises(DestinationDirectoryDoesntAllowOperation):
                self.bucket.download_file_by_name(self.file_version.file_name).save_to(target_file)


class TestFileInfoB2Fields(TestCaseWithBucket):
    @dataclasses.dataclass
    class TestCase:
        fields: dict[str, str]
        expires_dt: datetime.datetime | None = None
        expires_parsed: datetime.datetime | None = None
        expires_parsed_raises: bool = False

        @property
        def kwargs(self) -> dict[str, str | datetime.datetime]:
            kws = {**self.fields}
            if self.expires_dt:
                kws['expires'] = self.expires_dt
            return kws

        def assert_(self, version):
            for name, value in self.fields.items():
                assert getattr(version, name) == value

            if self.expires_parsed_raises:
                with pytest.raises(ValueError):
                    version.expires_parsed()
            else:
                assert version.expires_parsed() == self.expires_parsed

    test_cases = [
        TestCase(fields={}),
        TestCase(
            fields={
                'cache_control': 'max-age=3600',
                'expires': 'Sun, 06 Nov 1994 08:49:37 GMT',
                'content_disposition': 'attachment; filename="fname.ext"',
                'content_encoding': 'utf-8',
                'content_language': 'en_US',
            },
            expires_parsed=datetime.datetime(1994, 11, 6, 8, 49, 37, tzinfo=datetime.timezone.utc),
        ),
        # RFC 850 format
        TestCase(
            fields={'expires': 'Sunday, 06-Nov-95 08:49:37 GMT'},
            expires_parsed=datetime.datetime(1995, 11, 6, 8, 49, 37, tzinfo=datetime.timezone.utc),
        ),
        # ANSI C's asctime() format
        TestCase(
            fields={'expires': 'Sun Nov  6 08:49:37 1996'},
            expires_parsed=datetime.datetime(1996, 11, 6, 8, 49, 37, tzinfo=datetime.timezone.utc),
        ),
        # Non-standard date format
        TestCase(
            fields={'expires': '2020-01-01 00:00:00'},
            expires_parsed_raises=True,
        ),
        # Non-GMT timezone
        TestCase(
            fields={'expires': 'Sunday, 06-Nov-95 08:49:37 PDT'},
            expires_parsed_raises=True,
        ),
        # Passing `expires`` as a datetime
        TestCase(
            fields={'expires': 'Sun, 06 Nov 1994 08:49:37 GMT'},
            expires_dt=datetime.datetime(1994, 11, 6, 8, 49, 37, tzinfo=datetime.timezone.utc),
            expires_parsed=datetime.datetime(1994, 11, 6, 8, 49, 37, tzinfo=datetime.timezone.utc),
            expires_parsed_raises=False,
        ),
        # Passing `expires`` as a datetime in non-UTC timezone
        TestCase(
            fields={'expires': 'Sun, 06 Nov 1994 08:49:37 GMT'},
            expires_dt=datetime.datetime(
                1994, 11, 6, 9, 49, 37, tzinfo=datetime.timezone(datetime.timedelta(hours=1))
            ),
            expires_parsed=datetime.datetime(1994, 11, 6, 8, 49, 37, tzinfo=datetime.timezone.utc),
            expires_parsed_raises=False,
        ),
    ]

    def test_upload_bytes(self):
        for test_case in self.test_cases:
            file_version = self.bucket.upload_bytes(b'', 'file1', **test_case.kwargs)
            test_case.assert_(file_version)

    def test_upload_unbound_stream(self):
        for test_case in self.test_cases:
            file_version = self.bucket.upload_unbound_stream(
                io.BytesIO(b'data'), 'file1', **test_case.kwargs
            )
            test_case.assert_(file_version)

    def test_upload_empty_unbound_stream(self):
        for test_case in self.test_cases:
            file_version = self.bucket.upload_unbound_stream(
                io.BytesIO(b''), 'file1', **test_case.kwargs
            )
            test_case.assert_(file_version)

    def test_upload_local_file(self):
        for test_case in self.test_cases:
            with tempfile.TemporaryDirectory() as d:
                path = os.path.join(d, 'file1')
                data = b''
                write_file(path, data)
                file_version = self.bucket.upload_local_file(path, 'file1', **test_case.kwargs)
                test_case.assert_(file_version)

    def test_copy_with_file_info(self):
        for test_case in self.test_cases:
            file_version = self.bucket.upload_bytes(b'', 'file1', **test_case.kwargs)
            copied_file_version = self.bucket.copy(file_version.id_, 'file2')
            test_case.assert_(copied_file_version)

    def test_copy_overwriting_file_info(self):
        for test_case in self.test_cases:
            file_version = self.bucket.upload_bytes(b'', 'file1')
            # For reasons I don't know, the content type must be supplied if and only if
            # file info is supplied - otherwise the SDK raises an exception forbidding that.
            content_type = None
            if test_case.fields:
                content_type = 'text/plain'
            copied_file_version = self.bucket.copy(
                file_version.id_, 'file2', content_type=content_type, **test_case.kwargs
            )
            test_case.assert_(copied_file_version)

    def test_download_version(self):
        for test_case in self.test_cases:
            file_version = self.bucket.upload_bytes(b'', 'file1', **test_case.kwargs)
            download_file = self.bucket.download_file_by_id(file_version.id_)
            test_case.assert_(download_file.download_version)


# Listing where every other response returns no entries and pointer to the next file
class EmptyListBucketSimulator(BucketSimulator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Whenever we receive a list request, if it's the first time
        # for this particular ``start_file_name``, we'll return
        # an empty response pointing to the same file.
        self.last_queried_file = None

    def _should_return_empty(self, file_name: str) -> bool:
        # Note that every other request is empty – the logic is as follows:
        #   1st request – unknown start name – empty response
        #   2nd request – known start name – normal response with a proper next filename
        #   3rd request – unknown start name (as it's the next filename from the previous request) – empty response
        #   4th request – known start name
        # etc. This works especially well when using limiter of number of files fetched set to 1.
        should_return_empty = self.last_queried_file != file_name
        self.last_queried_file = file_name
        return should_return_empty

    def list_file_versions(
        self,
        account_auth_token,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None,  # noqa
        prefix=None,
    ):
        if self._should_return_empty(start_file_name):
            return dict(files=[], nextFileName=start_file_name, nextFileId=start_file_id)
        return super().list_file_versions(
            account_auth_token,
            start_file_name,
            start_file_id,
            1,  # Forcing only a single file per response.
            prefix,
        )

    def list_file_names(
        self,
        account_auth_token,
        start_file_name=None,
        max_file_count=None,  # noqa
        prefix=None,
    ):
        if self._should_return_empty(start_file_name):
            return dict(files=[], nextFileName=start_file_name)
        return super().list_file_names(
            account_auth_token,
            start_file_name,
            1,  # Forcing only a single file per response.
            prefix,
        )


class EmptyListSimulator(RawSimulator):
    BUCKET_SIMULATOR_CLASS = EmptyListBucketSimulator


class TestEmptyListVersions(TestListVersions):
    RAW_SIMULATOR_CLASS = EmptyListSimulator


class TestEmptyLs(TestLs):
    RAW_SIMULATOR_CLASS = EmptyListSimulator
