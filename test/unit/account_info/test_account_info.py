######################################################################
#
# File: test/unit/account_info/test_account_info.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import json
import os
import platform
import shutil
import stat
import sys
import tempfile
import unittest.mock as mock
from abc import ABCMeta, abstractmethod

import pytest
from apiver_deps import (
    ALL_CAPABILITIES,
    B2_ACCOUNT_INFO_ENV_VAR,
    XDG_CONFIG_HOME_ENV_VAR,
    AbstractAccountInfo,
    InMemoryAccountInfo,
    SqliteAccountInfo,
    UploadUrlPool,
)
from apiver_deps_exception import CorruptAccountInfo, MissingAccountData

from .fixtures import *  # noqa: F401, F403


class WindowsSafeTempDir(tempfile.TemporaryDirectory):
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            super().__exit__(exc_type, exc_val, exc_tb)
        except OSError:
            pass


class TestAccountInfo:
    @pytest.fixture(autouse=True)
    def setup(self, account_info_factory, account_info_default_data):
        self.account_info_factory = account_info_factory
        self.account_info_default_data = account_info_default_data

    @pytest.mark.parametrize(
        'application_key_id,realm,expected',
        (
            ('application_key_id', 'dev', True),
            ('application_key_id', 'test', False),
            ('different_application_key_id', 'dev', False),
            ('different_application_key_id', 'test', False),
        ),
    )
    def test_is_same_key(self, application_key_id, realm, expected):
        account_info = self.account_info_factory()
        account_info.set_auth_data(**self.account_info_default_data)

        assert account_info.is_same_key(application_key_id, realm) is expected

    @pytest.mark.parametrize(
        'account_id,application_key_id,expected',
        (
            ('account_id', 'account_id', True),
            ('account_id', 'ACCOUNT_ID', False),
            ('account_id', '123account_id0000000000', True),
            ('account_id', '234account_id0000000000', True),
            ('account_id', '123account_id000000000', False),
            ('account_id', '123account_id0000000001', False),
            ('account_id', '123account_id00000000000', False),
        ),
    )
    def test_is_master_key(self, account_id, application_key_id, expected):
        account_info = self.account_info_factory()
        account_data = self.account_info_default_data.copy()
        account_data['account_id'] = account_id
        account_data['application_key_id'] = application_key_id
        account_info.set_auth_data(**account_data)

        assert account_info.is_master_key() is expected, (account_id, application_key_id, expected)

    @pytest.mark.parametrize(
        'account_id,realm,expected',
        (
            ('account_id', 'dev', True),
            ('account_id', 'test', False),
            ('different_account_id', 'dev', False),
            ('different_account_id', 'test', False),
        ),
    )
    def test_is_same_account(self, account_id, realm, expected):
        account_info = self.account_info_factory()
        account_info.set_auth_data(**self.account_info_default_data)

        assert account_info.is_same_account(account_id, realm) is expected

    @pytest.mark.parametrize(
        's3_api_url',
        ('https://s3.us-east-123.backblazeb2.com', 'https://s3.us-west-321.backblazeb2.com')
    )
    def test_s3_api_url(self, s3_api_url):
        account_info = self.account_info_factory()
        account_info_default_data = {
            **self.account_info_default_data,
            's3_api_url': s3_api_url,
        }
        account_info.set_auth_data(**account_info_default_data)
        assert s3_api_url == account_info.get_s3_api_url()

    def test_getting_all_capabilities(self):
        account_info = self.account_info_factory()
        assert account_info.all_capabilities() == ALL_CAPABILITIES


class TestUploadUrlPool:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.pool = UploadUrlPool()

    def test_take_empty(self):
        assert (None, None) == self.pool.take('a')

    def test_put_and_take(self):
        self.pool.put('a', 'url_a1', 'auth_token_a1')
        self.pool.put('a', 'url_a2', 'auth_token_a2')
        self.pool.put('b', 'url_b1', 'auth_token_b1')
        assert ('url_a2', 'auth_token_a2') == self.pool.take('a')
        assert ('url_a1', 'auth_token_a1') == self.pool.take('a')
        assert (None, None) == self.pool.take('a')
        assert ('url_b1', 'auth_token_b1') == self.pool.take('b')
        assert (None, None) == self.pool.take('b')

    def test_clear(self):
        self.pool.put('a', 'url_a1', 'auth_token_a1')
        self.pool.clear_for_key('a')
        self.pool.put('b', 'url_b1', 'auth_token_b1')
        assert (None, None) == self.pool.take('a')
        assert ('url_b1', 'auth_token_b1') == self.pool.take('b')
        assert (None, None) == self.pool.take('b')


class AccountInfoBase(metaclass=ABCMeta):
    # it is a mixin to avoid running the tests directly (without inheritance)
    PERSISTENCE = NotImplemented  # subclass should override this

    @abstractmethod
    def _make_info(self):
        """
        returns a new object of AccountInfo class which should be tested
        """

    def test_clear(self, account_info_default_data, apiver):
        account_info = self._make_info()
        account_info.set_auth_data(**account_info_default_data)
        account_info.clear()

        with pytest.raises(MissingAccountData):
            account_info.get_account_id()
        with pytest.raises(MissingAccountData):
            account_info.get_account_auth_token()
        with pytest.raises(MissingAccountData):
            account_info.get_api_url()
        with pytest.raises(MissingAccountData):
            account_info.get_application_key()
        with pytest.raises(MissingAccountData):
            account_info.get_download_url()
        with pytest.raises(MissingAccountData):
            account_info.get_realm()
        with pytest.raises(MissingAccountData):
            account_info.get_application_key_id()
        assert not account_info.is_same_key('key_id', 'realm')

        if apiver in ['v0', 'v1']:
            with pytest.raises(MissingAccountData):
                account_info.get_minimum_part_size()
        else:
            with pytest.raises(MissingAccountData):
                account_info.get_recommended_part_size()
            with pytest.raises(MissingAccountData):
                account_info.get_absolute_minimum_part_size()

    def test_set_auth_data_compatibility(self, account_info_default_data):
        account_info = self._make_info()

        # The original set_auth_data
        account_info.set_auth_data(**account_info_default_data)
        actual = account_info.get_allowed()
        assert AbstractAccountInfo.DEFAULT_ALLOWED == actual, 'default allowed'

        # allowed was added later
        allowed = dict(
            bucketId=None,
            bucketName=None,
            capabilities=['readFiles'],
            namePrefix=None,
        )
        account_info.set_auth_data(**{
            **account_info_default_data,
            'allowed': allowed,
        })
        assert allowed == account_info.get_allowed()

    def test_clear_bucket_upload_data(self):
        account_info = self._make_info()
        account_info.put_bucket_upload_url('bucket-0', 'http://bucket-0', 'bucket-0_auth')
        account_info.clear_bucket_upload_data('bucket-0')
        assert (None, None) == account_info.take_bucket_upload_url('bucket-0')

    def test_large_file_upload_urls(self):
        account_info = self._make_info()
        account_info.put_large_file_upload_url('file_0', 'http://file_0', 'auth_0')
        assert ('http://file_0', 'auth_0') == account_info.take_large_file_upload_url('file_0')
        assert (None, None) == account_info.take_large_file_upload_url('file_0')

    def test_clear_large_file_upload_urls(self):
        account_info = self._make_info()
        account_info.put_large_file_upload_url('file_0', 'http://file_0', 'auth_0')
        account_info.clear_large_file_upload_urls('file_0')
        assert (None, None) == account_info.take_large_file_upload_url('file_0')

    def test_bucket(self):
        account_info = self._make_info()
        bucket = mock.MagicMock()
        bucket.name = 'my-bucket'
        bucket.id_ = 'bucket-0'
        assert account_info.get_bucket_id_or_none_from_bucket_name('my-bucket') is None
        assert account_info.get_bucket_name_or_none_from_bucket_id('bucket-0') is None
        account_info.save_bucket(bucket)
        assert 'bucket-0' == account_info.get_bucket_id_or_none_from_bucket_name('my-bucket')
        assert 'my-bucket' == account_info.get_bucket_name_or_none_from_bucket_id('bucket-0')
        if self.PERSISTENCE:
            assert 'bucket-0' == self._make_info(
            ).get_bucket_id_or_none_from_bucket_name('my-bucket')
            assert 'my-bucket' == self._make_info(
            ).get_bucket_name_or_none_from_bucket_id('bucket-0')
        assert ('my-bucket', 'bucket-0') in account_info.list_bucket_names_ids()
        account_info.remove_bucket_name('my-bucket')
        assert account_info.get_bucket_id_or_none_from_bucket_name('my-bucket') is None
        assert account_info.get_bucket_name_or_none_from_bucket_id('bucket-0') is None
        assert ('my-bucket', 'bucket-0') not in account_info.list_bucket_names_ids()
        if self.PERSISTENCE:
            assert self._make_info().get_bucket_id_or_none_from_bucket_name('my-bucket') is None
            assert self._make_info().get_bucket_name_or_none_from_bucket_id('bucket-0') is None

    def test_refresh_bucket(self):
        account_info = self._make_info()
        assert account_info.get_bucket_id_or_none_from_bucket_name('my-bucket') is None
        assert account_info.get_bucket_name_or_none_from_bucket_id('a') is None
        bucket_names = {'a': 'bucket-0', 'b': 'bucket-1'}
        account_info.refresh_entire_bucket_name_cache(bucket_names.items())
        assert 'bucket-0' == account_info.get_bucket_id_or_none_from_bucket_name('a')
        assert 'a' == account_info.get_bucket_name_or_none_from_bucket_id('bucket-0')
        if self.PERSISTENCE:
            assert 'bucket-0' == self._make_info().get_bucket_id_or_none_from_bucket_name('a')
            assert 'a' == self._make_info().get_bucket_name_or_none_from_bucket_id('bucket-0')

    @pytest.mark.apiver(to_ver=1)
    def test_account_info_up_to_v1(self):
        account_info = self._make_info()
        account_info.set_auth_data(
            'account_id',
            'account_auth',
            'https://api.backblazeb2.com',
            'download_url',
            100,
            'app_key',
            'realm',
            application_key_id='key_id'
        )

        object_instances = [account_info]
        if self.PERSISTENCE:
            object_instances.append(self._make_info())
        for info2 in object_instances:
            assert 'account_id' == info2.get_account_id()
            assert 'account_auth' == info2.get_account_auth_token()
            assert 'https://api.backblazeb2.com' == info2.get_api_url()
            assert 'app_key' == info2.get_application_key()
            assert 'key_id' == info2.get_application_key_id()
            assert 'realm' == info2.get_realm()
            assert 100 == info2.get_minimum_part_size()
            assert info2.is_same_key('key_id', 'realm')
            assert not info2.is_same_key('key_id', 'another_realm')
            assert not info2.is_same_key('another_key_id', 'realm')
            assert not info2.is_same_key('another_key_id', 'another_realm')

    @pytest.mark.apiver(from_ver=2)
    def test_account_info_v2(self):
        account_info = self._make_info()
        account_info.set_auth_data(
            account_id='account_id',
            auth_token='account_auth',
            api_url='https://api.backblazeb2.com',
            download_url='download_url',
            recommended_part_size=100,
            absolute_minimum_part_size=50,
            application_key='app_key',
            realm='realm',
            s3_api_url='s3_api_url',
            allowed=None,
            application_key_id='key_id',
        )

        object_instances = [account_info]
        if self.PERSISTENCE:
            object_instances.append(self._make_info())
        for info2 in object_instances:
            assert 'account_id' == info2.get_account_id()
            assert 'account_auth' == info2.get_account_auth_token()
            assert 'https://api.backblazeb2.com' == info2.get_api_url()
            assert 'app_key' == info2.get_application_key()
            assert 'key_id' == info2.get_application_key_id()
            assert 'realm' == info2.get_realm()
            assert 100 == info2.get_recommended_part_size()
            assert 50 == info2.get_absolute_minimum_part_size()
            assert info2.is_same_key('key_id', 'realm')
            assert not info2.is_same_key('key_id', 'another_realm')
            assert not info2.is_same_key('another_key_id', 'realm')
            assert not info2.is_same_key('another_key_id', 'another_realm')


class TestInMemoryAccountInfo(AccountInfoBase):
    PERSISTENCE = False

    def _make_info(self):
        return InMemoryAccountInfo()


class TestSqliteAccountInfo(AccountInfoBase):
    PERSISTENCE = True

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        self.db_path = tempfile.NamedTemporaryFile(
            prefix=f'tmp_b2_tests_{request.node.name}__', delete=True
        ).name
        try:
            os.unlink(self.db_path)
        except OSError:
            pass
        self.test_home = tempfile.mkdtemp()

        yield

        for cleanup_method in [
            lambda: os.unlink(self.db_path), lambda: shutil.rmtree(self.test_home)
        ]:
            try:
                cleanup_method()
            except OSError:
                pass

    @pytest.mark.skipif(
        platform.system() == 'Windows',
        reason='different permission system on Windows',
    )
    def test_permissions(self):
        """
        Test that a new database won't be readable by just any user
        """
        SqliteAccountInfo(file_name=self.db_path,)
        mode = os.stat(self.db_path).st_mode
        assert stat.filemode(mode) == '-rw-------'

    def test_corrupted(self):
        """
        Test that a corrupted file will be replaced with a blank file.
        """
        with open(self.db_path, 'wb') as f:
            f.write(b'not a valid database')

        with pytest.raises(CorruptAccountInfo):
            self._make_info()

    @pytest.mark.skipif(
        platform.system() == 'Windows',
        reason='it fails to upgrade on Windows, not worth to fix it anymore'
    )
    def test_convert_from_json(self):
        """
        Tests converting from a JSON account info file, which is what version
        0.5.2 of the command-line tool used.
        """
        data = dict(
            account_auth_token='auth_token',
            account_id='account_id',
            api_url='api_url',
            application_key='application_key',
            download_url='download_url',
            minimum_part_size=5000,
            realm='production'
        )
        with open(self.db_path, 'wb') as f:
            f.write(json.dumps(data).encode('utf-8'))
        account_info = self._make_info()
        assert 'auth_token' == account_info.get_account_auth_token()

    def _make_info(self):
        return self._make_sqlite_account_info()

    def _make_sqlite_account_info(self, env=None, last_upgrade_to_run=None):
        """
        Returns a new SqliteAccountInfo that has just read the data from the file.

        :param dict env: Override Environment variables.
        """
        # Override HOME to ensure hermetic tests
        with mock.patch('os.environ', env or {'HOME': self.test_home}):
            return SqliteAccountInfo(
                file_name=self.db_path if not env else None,
                last_upgrade_to_run=last_upgrade_to_run,
            )

    def test_uses_xdg_config_home(self, apiver):
        is_xdg_os = bool(SqliteAccountInfo._get_xdg_config_path())
        with WindowsSafeTempDir() as d:
            env = {
                'HOME': self.test_home,
                'USERPROFILE': self.test_home,
            }

            if is_xdg_os:
                # pass the env. variable on XDG-like OS only
                env[XDG_CONFIG_HOME_ENV_VAR] = d

            account_info = self._make_sqlite_account_info(env=env)
            if apiver in ['v0', 'v1']:
                expected_path = os.path.abspath(os.path.join(self.test_home, '.b2_account_info'))
            elif is_xdg_os:
                assert os.path.exists(os.path.join(d, 'b2'))
                expected_path = os.path.abspath(os.path.join(d, 'b2', 'account_info'))
            else:
                expected_path = os.path.abspath(os.path.join(self.test_home, '.b2_account_info'))
            actual_path = os.path.abspath(account_info.filename)
            assert expected_path == actual_path

    def test_uses_existing_file_and_ignores_xdg(self):
        with WindowsSafeTempDir() as d:
            default_db_file_location = os.path.join(self.test_home, '.b2_account_info')
            open(default_db_file_location, 'a').close()
            account_info = self._make_sqlite_account_info(
                env={
                    'HOME': self.test_home,
                    'USERPROFILE': self.test_home,
                    XDG_CONFIG_HOME_ENV_VAR: d,
                }
            )
            actual_path = os.path.abspath(account_info.filename)
            assert default_db_file_location == actual_path
            assert not os.path.exists(os.path.join(d, 'b2'))

    def test_account_info_env_var_overrides_xdg_config_home(self):
        with WindowsSafeTempDir() as d:
            account_info = self._make_sqlite_account_info(
                env={
                    'HOME': self.test_home,
                    'USERPROFILE': self.test_home,
                    XDG_CONFIG_HOME_ENV_VAR: d,
                    B2_ACCOUNT_INFO_ENV_VAR: os.path.join(d, 'b2_account_info'),
                }
            )
            expected_path = os.path.abspath(os.path.join(d, 'b2_account_info'))
            actual_path = os.path.abspath(account_info.filename)
            assert expected_path == actual_path

    def test_resolve_xdg_os_default(self):
        is_xdg_os = bool(SqliteAccountInfo._get_xdg_config_path())
        assert is_xdg_os == (sys.platform not in ('win32', 'darwin'))

    def test_resolve_xdg_os_default_no_env_var(self, monkeypatch):
        # ensure that XDG_CONFIG_HOME_ENV_VAR doesn't to resolve XDG-like OS
        monkeypatch.delenv(XDG_CONFIG_HOME_ENV_VAR, raising=False)

        is_xdg_os = bool(SqliteAccountInfo._get_xdg_config_path())
        assert is_xdg_os == (sys.platform not in ('win32', 'darwin'))
