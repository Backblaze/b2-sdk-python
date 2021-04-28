######################################################################
#
# File: test/unit/v0/test_file_metadata.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .deps import FileMetadata
from ..test_base import TestBase


def snake_to_camel(name):
    camel = ''.join(s.title() for s in name.split('_'))
    return camel[:1].lower() + camel[1:]


class TestFileMetadata(TestBase):
    KWARGS = {
        'file_id': '4_deadbeaf3b3e38a957f100d1e_f1042665d79618ae7_d20200903_m194254_c000_v0001053_t0048',
        'file_name': 'foo.txt',
        'content_type': 'text/plain',
        'content_length': '1',
        'content_sha1': '4518012e1b365e504001dbc94120624f15b8bbd5',
        'file_info': {},
    }  # yapf: disable
    INFO_DICT = {snake_to_camel(k): v for k, v in KWARGS.items()}

    def test_verified_sha1(self):
        metadata = FileMetadata(**self.KWARGS)

        self.assertTrue(metadata.content_sha1_verified)
        self.assertEqual(metadata.as_info_dict(), self.INFO_DICT)

    def test_unverified_sha1(self):
        kwargs = self.KWARGS.copy()
        kwargs['content_sha1'] = 'unverified:' + kwargs['content_sha1']
        info_dict = self.INFO_DICT.copy()
        info_dict['contentSha1'] = 'unverified:' + info_dict['contentSha1']
        metadata = FileMetadata(**kwargs)

        self.assertFalse(metadata.content_sha1_verified)
        self.assertEqual(metadata.as_info_dict(), info_dict)
