######################################################################
#
# File: test/integration/test_file_version_attributes.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime as dt

from .base import IntegrationTestBase


class TestFileVersionAttributes(IntegrationTestBase):
    def _assert_object_has_attributes(self, object, kwargs):
        for key, value in kwargs.items():
            assert getattr(object, key) == value

    def test_file_info_b2_attributes(self):
        # This test checks that attributes that are internally represented as file_info items with prefix `b2-`
        # are saved and retrieved correctly.

        bucket = self.create_bucket()
        expected_attributes = {
            'cache_control': 'max-age=3600',
            'expires': 'Wed, 21 Oct 2105 07:28:00 GMT',
            'content_disposition': 'attachment; filename="fname.ext"',
            'content_encoding': 'utf-8',
            'content_language': 'en',
        }
        kwargs = {
            **expected_attributes, 'expires':
                dt.datetime(2105, 10, 21, 7, 28, tzinfo=dt.timezone.utc)
        }

        file_version = bucket.upload_bytes(b'0', 'file', **kwargs)
        self._assert_object_has_attributes(file_version, expected_attributes)

        file_version = bucket.get_file_info_by_id(file_version.id_)
        self._assert_object_has_attributes(file_version, expected_attributes)

        download_file = bucket.download_file_by_id(file_version.id_)
        self._assert_object_has_attributes(download_file.download_version, expected_attributes)

        copied_version = bucket.copy(
            file_version.id_,
            'file_copy',
            content_type='text/plain',
            **{
                **kwargs, 'content_language': 'de'
            }
        )
        self._assert_object_has_attributes(
            copied_version, {
                **expected_attributes, 'content_language': 'de'
            }
        )
