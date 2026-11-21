######################################################################
#
# File: test/unit/bucket/test_regression_bucket.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Regression tests for Bucket and B2Api behaviour on the RawSimulator.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import json
import locale
import threading
from unittest import mock

import apiver_deps
import pytest
import requests
import responses
from apiver_deps import (
    AuthInfoCache,
    B2Api,
    B2Http,
    B2HttpApiConfig,
    B2RawHTTPApi,
    Filter,
    InMemoryAccountInfo,
    InMemoryCache,
    RawSimulator,
    SqliteAccountInfo,
    UploadSourceBytes,
    WriteIntent,
)
from apiver_deps_exception import CopyArgumentsMismatch, NonExistentBucket

from test.known_defect import DefectStillPresent, defect_raises, expect, known_defect

from .test_bucket import TestCaseWithBucket

pytestmark = [pytest.mark.apiver(from_ver=2)]


class TestRegressionCopyAndLs(TestCaseWithBucket):
    @known_defect(
        'C1',
        'copy() with expires/content_disposition/cache_control but no content_type is refused',
        raises=DefectStillPresent,
    )
    def test_c1_copy_with_expiry_but_no_content_type(self):
        """C1: copy() with expires/headers but no content_type must work (2.13.3 notes)."""
        file_id = self.bucket.upload_bytes(b'hello world', 'src.txt').id_
        with defect_raises(CopyArgumentsMismatch, match='content type'):
            copied = self.bucket.copy(
                file_id,
                'copied.txt',
                expires='Thu, 01 Dec 2050 16:00:00 GMT',
                content_disposition='attachment',
                cache_control='max-age=5',
            )
        assert copied.file_info['b2-expires'] == 'Thu, 01 Dec 2050 16:00:00 GMT', copied.file_info
        assert copied.file_info['b2-content-disposition'] == 'attachment', copied.file_info

    @known_defect(
        'C5',
        'ls("x") stops at an object named "x" and never lists "x/..."',
        condition=apiver_deps.V >= 3,
        raises=AssertionError,
    )
    def test_c5_ls_path_without_slash_lists_folder_next_to_same_named_file(self):
        """C5: ls('x') with an object 'x' and objects 'x/...' must also list 'x/...'."""
        self.bucket.upload_bytes(b'data', 'x')
        self.bucket.upload_bytes(b'data', 'x/1.txt')
        names = [fv.file_name for fv, _ in self.bucket.ls('x', recursive=True)]
        assert 'x/1.txt' in names, names

    @known_defect(
        'C5',
        'B2Folder for a prefix naming a single object hits an AssertionError',
        condition=apiver_deps.V >= 3,
        raises=AssertionError,
    )
    def test_c5_folder_sync_source_with_prefix_naming_a_single_object(self):
        """C5: B2Folder for 'b2://bucket/x' must not hit an AssertionError when only 'x' exists."""
        from apiver_deps import B2Folder

        self.bucket.upload_bytes(b'data', 'x')
        folder = B2Folder(self.bucket_name, 'x', self.api)
        # must not raise AssertionError (it may list nothing or the object, but not crash)
        list(folder.all_files(None))

    def test_c6_exact_filename_respects_exclude_filter(self):
        """C6: an exclude filter naming the exact file must hide it from ls()."""
        self.bucket.upload_bytes(b'hello world', 'b/a.txt')
        result = [
            fv.file_name
            for fv, _ in self.bucket.ls(
                'b/a.txt', recursive=True, filters=[Filter.exclude('b/a.txt')]
            )
        ]
        assert result == [], 'the filter was ignored'

    def test_c6_exact_filename_respects_include_filter_for_other_name(self):
        """C6: an include filter not matching the exact file must hide it from ls()."""
        self.bucket.upload_bytes(b'hello world', 'b/a.txt')
        result = [
            fv.file_name
            for fv, _ in self.bucket.ls(
                'b/a.txt', recursive=True, filters=[Filter.include('b/z.txt')]
            )
        ]
        assert result == [], 'the filter was ignored'


class TestRegressionUploadOptions(TestCaseWithBucket):
    def test_c45_custom_timestamp_is_kept_on_large_upload(self):
        """C45: a custom upload timestamp must be applied to multi-part uploads too."""
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        file_version = self.bucket.upload_bytes(data, 'large', custom_upload_timestamp=12345)
        assert file_version.upload_timestamp == 12345, file_version.upload_timestamp

    def test_c45_custom_timestamp_is_kept_on_large_unfinished_file(self):
        """C45: the started large file carries the custom timestamp (visible after the upload)."""
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        self.bucket.upload_bytes(data, 'large2', custom_upload_timestamp=777)
        stored = self.bucket.get_file_info_by_name('large2')
        assert stored.upload_timestamp == 777, stored.upload_timestamp

    def test_c45_interrupted_upload_preserves_timestamp_and_resumes(self):
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        for timestamp in (0, 777):
            with self.subTest(timestamp=timestamp):
                name = f'resume-{timestamp}'
                with mock.patch.object(
                    self.api.services.upload_manager,
                    'upload_part',
                    side_effect=RuntimeError('interrupted'),
                ):
                    with pytest.raises(RuntimeError, match='interrupted'):
                        self.bucket.upload_bytes(data, name, custom_upload_timestamp=timestamp)

                unfinished = list(self.bucket.list_unfinished_large_files(prefix=name))
                assert len(unfinished) == 1
                raw = self.simulator.list_unfinished_large_files(
                    self.api_url, self.account_auth_token, self.bucket_id, prefix=name
                )
                assert raw['files'][0]['uploadTimestamp'] == timestamp
                completed = self.bucket.upload_bytes(data, name, custom_upload_timestamp=timestamp)
                assert completed.id_ == unfinished[0].file_id
                assert completed.upload_timestamp == timestamp

    def test_c45_resume_matches_requested_timestamp(self):
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        for existing_timestamp, requested_timestamp in ((777, 777), (777, 888), (0, 0)):
            with self.subTest(existing=existing_timestamp, requested=requested_timestamp):
                name = f'match-{existing_timestamp}-{requested_timestamp}'
                unfinished = self.simulator.start_large_file(
                    self.api_url,
                    self.account_auth_token,
                    self.bucket_id,
                    name,
                    None,
                    {},
                    custom_upload_timestamp=existing_timestamp,
                )
                completed = self.bucket.upload_bytes(
                    data, name, custom_upload_timestamp=requested_timestamp
                )
                if existing_timestamp == requested_timestamp:
                    assert completed.id_ == unfinished['fileId']
                else:
                    assert completed.id_ != unfinished['fileId']
                assert completed.upload_timestamp == requested_timestamp

    def test_c45_explicit_resume_requires_matching_timestamp(self):
        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        for requested_timestamp in (777, 888):
            with self.subTest(requested=requested_timestamp):
                name = f'explicit-{requested_timestamp}'
                unfinished = self.simulator.start_large_file(
                    self.api_url,
                    self.account_auth_token,
                    self.bucket_id,
                    name,
                    None,
                    {'large_file_sha1': apiver_deps.hex_sha1_of_bytes(data)},
                    custom_upload_timestamp=777,
                )
                kwargs = dict(
                    write_intents=[WriteIntent(UploadSourceBytes(data))],
                    file_name=name,
                    continue_large_file_id=unfinished['fileId'],
                    custom_upload_timestamp=requested_timestamp,
                )
                if requested_timestamp == 777:
                    completed = self.bucket.create_file(**kwargs)
                    assert completed.id_ == unfinished['fileId']
                    assert completed.upload_timestamp == 777
                else:
                    with pytest.raises(ValueError, match='custom_upload_timestamp'):
                        self.bucket.create_file(**kwargs)
                    assert list(self.bucket.list_unfinished_large_files(prefix=name))

    @known_defect(
        'C33',
        'Bucket.upload passes min_part_size as the recommended part size instead of a lower limit',
        raises=DefectStillPresent,
    )
    def test_c33_upload_min_part_size_is_only_a_lower_limit(self):
        """C33: min_part_size is a lower limit; parts must still follow the recommended part size.

        A min_part_size below what the server accepts must not shrink the parts below it: the
        planner has to keep using the account's recommended part size (not the user's minimum),
        otherwise the real service refuses the parts. Bucket.upload documents the first and does
        the second.
        """
        from unittest import mock

        absolute_minimum = self.account_info.get_absolute_minimum_part_size()
        recommended_part_size = absolute_minimum * 3
        user_minimum = absolute_minimum // 2
        part_lengths = []
        original = self.simulator.upload_part

        def recording(upload_url, token, part_number, content_length, *args, **kwargs):
            part_lengths.append((part_number, content_length))
            return original(upload_url, token, part_number, content_length, *args, **kwargs)

        data = self._make_data(recommended_part_size * 4 + 1)
        with mock.patch.object(
            self.account_info, 'get_recommended_part_size', return_value=recommended_part_size
        ), mock.patch.object(self.simulator, 'upload_part', recording):
            self.bucket.upload(UploadSourceBytes(data), 'min-part.bin', min_part_size=user_minimum)
        self._check_file_contents('min-part.bin', data)
        assert len(part_lengths) > 2, f'the upload was not split into several parts: {part_lengths}'
        non_final = [length for _, length in sorted(part_lengths)[:-1]]
        assert all(length >= user_minimum for length in non_final), non_final
        expect(
            all(length >= recommended_part_size for length in non_final),
            f'parts below the recommended part size {recommended_part_size}: {non_final}',
        )

    @pytest.mark.skipif(
        not hasattr(locale, 'setlocale'), reason='locale support is required for this test'
    )
    @known_defect(
        'C32',
        'the "expires" header uses day and month names of the process locale',
        raises=AssertionError,
    )
    def test_c32_expires_header_uses_english_names_under_foreign_locale(self):
        """C32: the 'expires' date must not use process-locale day/month names."""
        old = locale.setlocale(locale.LC_TIME)
        for name in ('de_DE.UTF-8', 'de_DE.utf8', 'fr_FR.UTF-8', 'es_ES.UTF-8'):
            try:
                locale.setlocale(locale.LC_TIME, name)
                break
            except locale.Error:
                continue
        else:
            pytest.skip('no non-English locale available on this host')
        try:
            expires = datetime.datetime(2050, 12, 1, 16, 0, tzinfo=datetime.timezone.utc)
            file_version = self.bucket.upload_bytes(b'x', 'f.txt', expires=expires)
        finally:
            locale.setlocale(locale.LC_TIME, old)
        assert (
            file_version.file_info['b2-expires'] == 'Thu, 01 Dec 2050 16:00:00 GMT'
        ), file_version.file_info


class TestRegressionApi:
    def _api(self, account_info=None, cache=None):
        api = B2Api(
            account_info or InMemoryAccountInfo(),
            cache=cache if cache is not None else InMemoryCache(),
            api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
        )
        account_id, master_key = api.session.raw_api.create_account()
        api.authorize_account(
            application_key_id=account_id, application_key=master_key, realm='production'
        )
        return api, master_key

    @pytest.mark.parametrize('cache_kind', ['memory', 'account_info', 'sqlite'])
    def test_c2_deleted_bucket_not_returned_from_name_cache(self, cache_kind, tmp_path):
        """C2: after delete_bucket, a lookup by name must fail, not return the stale ID."""
        account_info = (
            SqliteAccountInfo(file_name=str(tmp_path / 'account.sqlite'))
            if cache_kind == 'sqlite'
            else InMemoryAccountInfo()
        )
        cache = InMemoryCache() if cache_kind == 'memory' else AuthInfoCache(account_info)
        api, _ = self._api(account_info=account_info, cache=cache)
        bucket = api.create_bucket('to-delete', 'allPrivate')
        assert api.get_bucket_by_name('to-delete').id_ == bucket.id_, 'cache was not warm'
        api.delete_bucket(bucket)
        with pytest.raises(NonExistentBucket):
            api.get_bucket_by_name('to-delete')

    def test_failed_bucket_delete_preserves_cached_name(self, monkeypatch):
        api, _ = self._api()
        bucket = api.create_bucket('not-deleted', 'allPrivate')

        def fail_delete(*args):
            raise RuntimeError('delete failed')

        monkeypatch.setattr(api.session, 'delete_bucket', fail_delete)
        with pytest.raises(RuntimeError, match='delete failed'):
            api.delete_bucket(bucket)
        assert api.cache.get_bucket_id_or_none_from_bucket_name(bucket.name) == bucket.id_

    @known_defect(
        'C28',
        'the create_bucket mismatch message prints the same name twice',
        raises=AssertionError,
    )
    def test_c28_create_bucket_mismatch_message_names_both_buckets(self):
        """C28: the assertion message must show requested and returned names, not name twice."""
        from unittest import mock

        api, _ = self._api()
        wrong = mock.Mock()
        wrong.name = 'returned-other-name'
        wrong.type_ = 'allPrivate'
        with mock.patch.object(
            api.BUCKET_FACTORY_CLASS, 'from_api_bucket_dict', return_value=wrong
        ):
            with pytest.raises(AssertionError) as exc_info:
                api.create_bucket('requested-name', 'allPrivate')
        message = str(exc_info.value)
        assert 'requested-name' in message, message
        assert 'returned-other-name' in message, message


class TestRegressionRawApiContract:
    @responses.activate
    @known_defect(
        'C35',
        'set_notification_rules fails for a generator although it is typed as Iterable',
        raises=DefectStillPresent,
    )
    def test_c35_set_notification_rules_accepts_a_generator(self):
        """C35: set_notification_rules is typed Iterable; a generator must serialize to JSON."""
        raw_api = B2RawHTTPApi(
            B2Http(B2HttpApiConfig(requests.Session, install_clock_skew_hook=False))
        )
        responses.add(
            responses.POST,
            f'https://api.example.com/b2api/{B2RawHTTPApi.API_VERSION}/b2_set_bucket_notification_rules',
            json={'eventNotificationRules': []},
        )
        rule = {
            'eventTypes': ['b2:ObjectCreated:*'],
            'isEnabled': True,
            'name': 'r',
            'objectNamePrefix': '',
            'targetConfiguration': {
                'targetType': 'webhook',
                'url': 'https://example.com/hook',
                'customHeaders': [],
            },
        }
        with defect_raises(TypeError, match='not JSON serializable'):
            sent = raw_api.set_bucket_notification_rules(
                'https://api.example.com', 'token', 'bucket-id', (r for r in [rule])
            )
        assert sent == [], sent
        body = json.loads(responses.calls[0].request.body)
        assert body['eventNotificationRules'] == [rule], body


class TestRegressionLargeUploadFailure:
    @known_defect(
        'C8',
        'part uploads keep running after one part of a large upload failed',
        raises=DefectStillPresent,
    )
    def test_c8_failed_part_stops_remaining_part_uploads(self):
        """C8: once one part fails the whole upload fails; queued parts must not keep uploading."""
        from unittest import mock

        from apiver_deps_exception import AccessDenied

        api = B2Api(
            InMemoryAccountInfo(),
            cache=InMemoryCache(),
            api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
            max_upload_workers=1,  # parts start one at a time, in order
        )
        simulator = api.session.raw_api
        account_id, master_key = simulator.create_account()
        api.authorize_account(
            application_key_id=account_id, application_key=master_key, realm='production'
        )
        bucket = api.create_bucket('big', 'allPrivate')
        original = simulator.upload_part
        part_failed = threading.Event()
        started_after_failure = []

        def flaky_upload_part(upload_url, token, part_number, *args, **kwargs):
            if part_failed.is_set():
                started_after_failure.append(part_number)
            elif part_number == 1:
                part_failed.set()  # before raising, so that no later start can be missed
                raise AccessDenied()
            return original(upload_url, token, part_number, *args, **kwargs)

        # collect the futures of the real part uploads, to wait for all of them to finish
        pool = api.services.upload_manager._thread_pool
        real_submit = pool.submit
        futures = []

        def recording_submit(*args, **kwargs):
            future = real_submit(*args, **kwargs)
            futures.append(future)
            return future

        data = b'x' * (simulator.MIN_PART_SIZE * 6)
        with mock.patch.object(simulator, 'upload_part', flaky_upload_part):
            with mock.patch.object(pool, 'submit', recording_submit):
                with pytest.raises(AccessDenied):
                    bucket.upload_bytes(data, 'big.bin')
                _, pending = concurrent.futures.wait(futures, timeout=30)

        assert part_failed.is_set(), 'part 1 was never uploaded, so no part failed'
        assert len(futures) > 1, 'the upload was not split into several parts'
        assert not pending, 'part uploads did not finish within the timeout'
        expect(
            not started_after_failure,
            f'parts kept uploading after the failure: {started_after_failure}',
        )


class TestRegressionDownloadedFileContract:
    @responses.activate
    def test_c56_downloaded_file_rejects_second_save_from_a_real_response(self):
        """A consumed HTTP response must be rejected with a clear error on re-save."""
        import hashlib
        import io
        from unittest import mock

        from apiver_deps import DownloadedFile, Range, SimpleDownloader

        data = b'0123456789' * 100
        url = 'https://download.example.com/file/bucket/name'
        responses.add(responses.GET, url, body=data, content_type='application/octet-stream')
        response = requests.get(url, stream=True)

        download_version = mock.MagicMock()
        download_version.content_length = len(data)
        download_version.content_sha1 = hashlib.sha1(data).hexdigest()
        download_version.content_encoding = None
        download_version.range_ = Range(0, len(data) - 1)
        download_version.api.api_config.decode_content = False
        download_manager = mock.MagicMock()
        download_manager.strategies = [SimpleDownloader(force_chunk_size=64)]

        downloaded = DownloadedFile(download_version, download_manager, None, response, None, None)
        first, second = io.BytesIO(), io.BytesIO()
        downloaded.save(first)
        assert first.getvalue() == data, 'first save() returned wrong data'
        with pytest.raises(RuntimeError, match='already been consumed'):
            downloaded.save(second)
        assert second.getvalue() == b'', 'rejected save() modified the destination'


class TestRegressionGuards(TestCaseWithBucket):
    def test_c43_guard_sse_c_key_is_not_printed_or_logged(self):
        """C43: SSE-C key material must not show in repr/str or DEBUG logs of public calls."""
        import logging

        from apiver_deps import (
            EncryptionAlgorithm,
            EncryptionKey,
            EncryptionMode,
            EncryptionSetting,
        )

        secret = b'sse-c-key-material-MUST-NOT-LEAK'
        setting = EncryptionSetting(
            mode=EncryptionMode.SSE_C,
            algorithm=EncryptionAlgorithm.AES256,
            key=EncryptionKey(secret=secret, key_id='key-id'),
        )
        for text in (repr(setting), str(setting), repr(setting.key), str(setting.key)):
            assert secret.decode() not in text, 'key material in printed form'
            assert repr(secret) not in text, 'key material in printed form'

        records = []

        class Collector(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = Collector(level=logging.DEBUG)
        root = logging.getLogger()
        old_level = root.level
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        try:
            fv = self.bucket.upload_bytes(b'data', 'enc.txt', encryption=setting)
            self.bucket.copy(
                fv.id_, 'enc-copy.txt', source_encryption=setting, destination_encryption=setting
            )
        finally:
            root.removeHandler(handler)
            root.setLevel(old_level)
        joined = '\n'.join(records)
        assert records, 'expected DEBUG records to be captured'
        assert secret.decode() not in joined, 'key material in DEBUG logs'
        assert repr(secret) not in joined, 'key material in DEBUG logs'

    def test_c13_guard_failed_part_upload_retries_with_a_fresh_upload_url(self):
        """C13: after a retryable part failure the retry must not reuse the failed URL/token."""
        from unittest import mock

        from .test_bucket import CanRetry

        failing_part = 2
        attempts = []
        original = self.simulator.upload_part

        def recording(upload_url, token, part_number, *args, **kwargs):
            attempts.append((part_number, upload_url, token))
            first_attempt = [a for a in attempts if a[0] == failing_part] == [attempts[-1]]
            if part_number == failing_part and first_attempt:
                raise CanRetry(True)  # RawSimulator only consumes set_upload_errors() for files
            return original(upload_url, token, part_number, *args, **kwargs)

        data = self._make_data(self.simulator.MIN_PART_SIZE * 3)
        with mock.patch.object(self.simulator, 'upload_part', recording):
            self.bucket.upload_bytes(data, 'large.bin')
        self._check_file_contents('large.bin', data)

        tried = [
            (url, token) for part_number, url, token in attempts if part_number == failing_part
        ]
        assert len(tried) >= 2, f'part {failing_part} was not retried: {tried}'
        assert tried[1] != tried[0], f'part {failing_part} retried with the failed URL/token'
