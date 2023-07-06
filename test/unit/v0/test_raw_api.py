######################################################################
#
# File: test/unit/v0/test_raw_api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest

from ..test_base import TestBase
from .deps import (
    B2Http,
    B2RawHTTPApi,
    BucketRetentionSetting,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    RetentionMode,
    RetentionPeriod,
)
from .deps_exception import UnusableFileName, WrongEncryptionModeForBucketDefault

# Unicode characters for testing filenames.  (0x0394 is a letter Delta.)
TWO_BYTE_UNICHR = chr(0x0394)
CHAR_UNDER_32 = chr(31)
DEL_CHAR = chr(127)


class TestRawAPIFilenames(TestBase):
    """Test that the filename checker passes conforming names and rejects those that don't."""

    def setUp(self):
        self.raw_api = B2RawHTTPApi(B2Http())

    def _should_be_ok(self, filename):
        """Call with test filenames that follow the filename rules.

        :param filename: unicode (or str) that follows the rules
        """
        print(f"Filename \"{filename}\" should be OK")
        self.assertTrue(self.raw_api.check_b2_filename(filename) is None)

    def _should_raise(self, filename, exception_message):
        """Call with filenames that don't follow the rules (so the rule checker should raise).

        :param filename: unicode (or str) that doesn't follow the rules
        :param exception_message: regexp that matches the exception's detailed message
        """
        print(
            "Filename \"{}\" should raise UnusableFileName(\".*{}.*\").".format(
                filename, exception_message
            )
        )
        with self.assertRaisesRegex(UnusableFileName, exception_message):
            self.raw_api.check_b2_filename(filename)

    def test_b2_filename_checker(self):
        """Test a conforming and non-conforming filename for each rule.

        From the B2 docs (https://www.backblaze.com/b2/docs/files.html):
        - Names can be pretty much any UTF-8 string up to 1024 bytes long.
        - No character codes below 32 are allowed.
        - Backslashes are not allowed.
        - DEL characters (127) are not allowed.
        - File names cannot start with "/", end with "/", or contain "//".
        - Maximum of 250 bytes of UTF-8 in each segment (part between slashes) of a file name.
        """
        print("test b2 filename rules")

        # Examples from doc:
        self._should_be_ok('Kitten Videos')
        self._should_be_ok('\u81ea\u7531.txt')

        # Check length
        # 1024 bytes is ok if the segments are at most 250 chars.
        s_1024 = 4 * (250 * 'x' + '/') + 20 * 'y'
        self._should_be_ok(s_1024)
        # 1025 is too long.
        self._should_raise(s_1024 + 'x', "too long")
        # 1024 bytes with two byte characters should also work.
        s_1024_two_byte = 4 * (125 * TWO_BYTE_UNICHR + '/') + 20 * 'y'
        self._should_be_ok(s_1024_two_byte)
        # But 1025 bytes is too long.
        self._should_raise(s_1024_two_byte + 'x', "too long")

        # Names with unicode values < 32, and DEL aren't allowed.
        self._should_raise('hey' + CHAR_UNDER_32, "contains code.*less than 32")
        # Unicode in the filename shouldn't break the exception message.
        self._should_raise(TWO_BYTE_UNICHR + CHAR_UNDER_32, "contains code.*less than 32")
        self._should_raise(DEL_CHAR, "DEL.*not allowed")

        # Names can't start or end with '/' or contain '//'
        self._should_raise('/hey', "not start.*/")
        self._should_raise('hey/', "not .*end.*/")
        self._should_raise('not//allowed', "contain.*//")

        # Reject segments longer than 250 bytes
        self._should_raise('foo/' + 251 * 'x', "segment too long")
        # So a segment of 125 two-byte chars plus one should also fail.
        self._should_raise('foo/' + 125 * TWO_BYTE_UNICHR + 'x', "segment too long")


class BucketTestBase:
    @pytest.fixture(autouse=True)
    def init(self, mocker):
        b2_http = mocker.MagicMock()
        self.raw_api = B2RawHTTPApi(b2_http)


class TestUpdateBucket(BucketTestBase):
    """Test updating bucket."""

    @pytest.fixture(autouse=True)
    def init(self, mocker):
        b2_http = mocker.MagicMock()
        self.raw_api = B2RawHTTPApi(b2_http)

    def test_assertion_raises(self):
        with pytest.raises(AssertionError):
            self.raw_api.update_bucket('test', 'account_auth_token', 'account_id', 'bucket_id')

    @pytest.mark.parametrize(
        'bucket_type,bucket_info,default_retention', (
            (None, {}, None),
            (
                'allPublic', None,
                BucketRetentionSetting(RetentionMode.COMPLIANCE, RetentionPeriod(years=1))
            ),
        )
    )
    def test_assertion_not_raises(self, bucket_type, bucket_info, default_retention):
        self.raw_api.update_bucket(
            'test',
            'account_auth_token',
            'account_id',
            'bucket_id',
            bucket_type=bucket_type,
            bucket_info=bucket_info,
            default_retention=default_retention,
        )

    @pytest.mark.parametrize(
        'encryption_setting,', (
            EncryptionSetting(
                mode=EncryptionMode.SSE_C,
                algorithm=EncryptionAlgorithm.AES256,
                key=EncryptionKey(b'key', 'key-id')
            ),
            EncryptionSetting(mode=EncryptionMode.UNKNOWN,),
        )
    )
    def test_update_bucket_wrong_encryption(self, encryption_setting):
        with pytest.raises(WrongEncryptionModeForBucketDefault):
            self.raw_api.update_bucket(
                'test',
                'account_auth_token',
                'account_id',
                'bucket_id',
                default_server_side_encryption=encryption_setting,
                bucket_type='allPublic',
            )


class TestCreateBucket(BucketTestBase):
    """Test creating bucket."""

    @pytest.mark.parametrize(
        'encryption_setting,', (
            EncryptionSetting(
                mode=EncryptionMode.SSE_C,
                algorithm=EncryptionAlgorithm.AES256,
                key=EncryptionKey(b'key', 'key-id')
            ),
            EncryptionSetting(mode=EncryptionMode.UNKNOWN,),
        )
    )
    def test_create_bucket_wrong_encryption(self, encryption_setting):

        with pytest.raises(WrongEncryptionModeForBucketDefault):
            self.raw_api.create_bucket(
                'test',
                'account_auth_token',
                'account_id',
                'bucket_id',
                bucket_type='allPrivate',
                bucket_info={},
                default_server_side_encryption=encryption_setting,
            )
