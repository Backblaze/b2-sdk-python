######################################################################
#
# File: test/unit/file_version/test_file_version.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

import apiver_deps
from apiver_deps import B2Api
from apiver_deps import B2HttpApiConfig
from apiver_deps import DummyCache
from apiver_deps import InMemoryAccountInfo
from apiver_deps import LegalHold
from apiver_deps import RawSimulator

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
        self.api.authorize_account('production', self.application_key_id, self.master_key)

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
        self.bucket = self.api.create_bucket('testbucket', 'allPrivate', is_file_lock_enabled=True)
        initial_file_version = self.bucket.upload_bytes(b'nothing', 'test_file')
        self.api.update_file_legal_hold(
            initial_file_version.id_, initial_file_version.file_name, LegalHold.ON
        )
        fetched_version = self.api.get_file_info(initial_file_version.id_)
        if apiver_deps.V <= 1:
            fetched_version = self.api.file_version_factory.from_api_response(fetched_version)
        assert initial_file_version.as_dict() != fetched_version.as_dict()
        refreshed_version = initial_file_version.get_fresh_state()
        assert isinstance(refreshed_version, VFileVersion)
        assert refreshed_version.as_dict() == fetched_version.as_dict()
