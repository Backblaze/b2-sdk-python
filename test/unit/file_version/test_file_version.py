######################################################################
#
# File: test/unit/file_version/test_file_version.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import time

import apiver_deps
import pytest
from apiver_deps import (
    B2Api,
    B2HttpApiConfig,
    DownloadVersion,
    DummyCache,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FileIdAndName,
    FileRetentionSetting,
    InMemoryAccountInfo,
    LegalHold,
    RawSimulator,
    RetentionMode,
)
from apiver_deps_exception import AccessDenied, FileNotPresent

if apiver_deps.V <= 1:
    from apiver_deps import FileVersionInfo as VFileVersion
else:
    from apiver_deps import FileVersion as VFileVersion


class TestFileVersion:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.account_info = InMemoryAccountInfo()
        self.cache = DummyCache()
        self.api = B2Api(
            self.account_info, self.cache, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.api.session.raw_api
        (self.application_key_id, self.master_key) = self.raw_api.create_account()
        self.api.authorize_account(
            application_key_id=self.application_key_id,
            application_key=self.master_key,
            realm='production',
        )
        self.bucket = self.api.create_bucket('testbucket', 'allPrivate', is_file_lock_enabled=True)
        self.file_version = self.bucket.upload_bytes(
            b'nothing', 'test_file', cache_control='private, max-age=3600'
        )

    @pytest.mark.apiver(to_ver=1)
    def test_format_ls_entry(self):
        file_version_info = VFileVersion(
            'a2', 'inner/a.txt', 200, 'text/plain', 'sha1', {}, 2000, 'upload'
        )
        expected_entry = (
            '                                                       '
            '                          a2  upload  1970-01-01  '
            '00:00:02        200  inner/a.txt'
        )
        assert expected_entry == file_version_info.format_ls_entry()

    def test_get_fresh_state(self):
        self.api.update_file_legal_hold(
            self.file_version.id_, self.file_version.file_name, LegalHold.ON
        )
        fetched_version = self.api.get_file_info(self.file_version.id_)
        if apiver_deps.V <= 1:
            fetched_version = self.api.file_version_factory.from_api_response(fetched_version)
        assert self.file_version.as_dict() != fetched_version.as_dict()
        refreshed_version = self.file_version.get_fresh_state()
        assert isinstance(refreshed_version, VFileVersion)
        assert refreshed_version.as_dict() == fetched_version.as_dict()

    def test_clone_file_version_and_download_version(self):
        encryption = EncryptionSetting(
            EncryptionMode.SSE_C, EncryptionAlgorithm.AES256, EncryptionKey(b'secret', None)
        )
        initial_file_version = self.bucket.upload_bytes(
            b'nothing',
            'test_file',
            content_type='video/mp4',
            file_info={
                'file': 'info',
                'b2-content-language': 'en_US',
                'b2-content-disposition': 'attachment',
                'b2-expires': '2100-01-01',
                'b2-content-encoding': 'text',
            },
            encryption=encryption,
            file_retention=FileRetentionSetting(RetentionMode.GOVERNANCE, 100),
            legal_hold=LegalHold.ON,
            cache_control='public, max-age=86400',
        )
        assert initial_file_version._clone() == initial_file_version
        cloned = initial_file_version._clone(legal_hold=LegalHold.OFF)
        assert isinstance(cloned, VFileVersion)
        assert cloned.as_dict() == {
            **initial_file_version.as_dict(), 'legalHold': LegalHold.OFF.value
        }

        download_version = self.api.download_file_by_id(
            initial_file_version.id_, encryption=encryption
        ).download_version
        assert download_version._clone() == download_version
        cloned = download_version._clone(legal_hold=LegalHold.OFF)
        assert isinstance(cloned, DownloadVersion)
        assert cloned.as_dict() == {**download_version.as_dict(), 'legalHold': LegalHold.OFF.value}

    def test_update_legal_hold(self):
        new_file_version = self.file_version.update_legal_hold(LegalHold.ON)
        assert isinstance(new_file_version, VFileVersion)
        assert new_file_version.legal_hold == LegalHold.ON

        download_version = self.api.download_file_by_id(self.file_version.id_).download_version
        new_download_version = download_version.update_legal_hold(LegalHold.ON)
        assert isinstance(new_download_version, DownloadVersion)
        assert new_download_version.legal_hold == LegalHold.ON

    def test_update_retention(self):
        new_retention = FileRetentionSetting(RetentionMode.COMPLIANCE, 100)

        new_file_version = self.file_version.update_retention(new_retention)
        assert isinstance(new_file_version, VFileVersion)
        assert new_file_version.file_retention == new_retention

        download_version = self.api.download_file_by_id(self.file_version.id_).download_version
        new_download_version = download_version.update_retention(new_retention)
        assert isinstance(new_download_version, DownloadVersion)
        assert new_download_version.file_retention == new_retention

    def test_delete_file_version(self):
        ret = self.file_version.delete()
        assert isinstance(ret, FileIdAndName)
        with pytest.raises(FileNotPresent):
            self.bucket.get_file_info_by_name(self.file_version.file_name)

    def test_delete_bypass_governance(self):
        locked_file_version = self.bucket.upload_bytes(
            b'nothing',
            'test_file_with_governance',
            file_retention=FileRetentionSetting(RetentionMode.GOVERNANCE,
                                                int(time.time()) + 100),
        )

        with pytest.raises(AccessDenied):
            locked_file_version.delete()

        locked_file_version.delete(bypass_governance=True)
        with pytest.raises(FileNotPresent):
            self.bucket.get_file_info_by_name(locked_file_version.file_name)

    def test_delete_download_version(self):
        download_version = self.api.download_file_by_id(self.file_version.id_).download_version
        ret = download_version.delete()
        assert isinstance(ret, FileIdAndName)
        with pytest.raises(FileNotPresent):
            self.bucket.get_file_info_by_name(self.file_version.file_name)

    def test_file_version_upload_headers(self):
        file_version = self.file_version._clone(
            server_side_encryption=EncryptionSetting(
                EncryptionMode.SSE_C,
                EncryptionAlgorithm.AES256,
                EncryptionKey(None, None),
            ),
        )

        assert file_version._get_upload_headers() == """
            Authorization: auth_token_0
            Content-Length: 7
            X-Bz-File-Name: test_file
            Content-Type: b2/x-auto
            X-Bz-Content-Sha1: 0feca720e2c29dafb2c900713ba560e03b758711
            X-Bz-Info-b2-cache-control: private%2C%20max-age%3D3600
            X-Bz-Server-Side-Encryption-Customer-Algorithm: AES256
            X-Bz-Server-Side-Encryption-Customer-Key: KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=
            X-Bz-Server-Side-Encryption-Customer-Key-Md5: SaaDheEjzuynJH8eW6AEpQ==
            X-Bz-File-Legal-Hold: off
            X-Bz-File-Retention-Mode: None
            X-Bz-File-Retention-Retain-Until-Timestamp: None
        """.strip().replace(': ', '').replace(' ', '').replace('\n', '').encode('utf8')

        assert not file_version.has_large_header

        file_version.file_info['dummy'] = 'a' * 2000  # make metadata > 2k bytes
        assert file_version.has_large_header

    # FileVersion.download tests are not here, because another test file already has all the facilities for such test
    # prepared
