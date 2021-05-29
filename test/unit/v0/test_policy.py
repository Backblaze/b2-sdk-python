######################################################################
#
# File: test/unit/v0/test_policy.py
#
# Copyright 2019, Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from unittest.mock import MagicMock

from ..test_base import TestBase

from .deps import FileVersionInfo
from .deps import LocalSyncPath, B2SyncPath
from .deps import B2Folder
from .deps import make_b2_keep_days_actions


class TestMakeB2KeepDaysActions(TestBase):
    def setUp(self):
        self.keep_days = 7
        self.today = 100 * 86400
        self.one_day_millis = 86400 * 1000

    def test_no_versions(self):
        self.check_one_answer(True, [], [])

    def test_new_version_no_action(self):
        self.check_one_answer(True, [(1, -5, 'upload')], [])

    def test_no_source_one_old_version_hides(self):
        # An upload that is old gets deleted if there is no source file.
        self.check_one_answer(False, [(1, -10, 'upload')], ['b2_hide(folder/a)'])

    def test_old_hide_causes_delete(self):
        # A hide marker that is old gets deleted, as do the things after it.
        self.check_one_answer(
            True, [(1, -5, 'upload'), (2, -10, 'hide'), (3, -20, 'upload')],
            ['b2_delete(folder/a, 2, (hide marker))', 'b2_delete(folder/a, 3, (old version))']
        )

    def test_old_upload_causes_delete(self):
        # An upload that is old stays if there is a source file, but things
        # behind it go away.
        self.check_one_answer(
            True, [(1, -5, 'upload'), (2, -10, 'upload'), (3, -20, 'upload')],
            ['b2_delete(folder/a, 3, (old version))']
        )

    def test_out_of_order_dates(self):
        # The one at date -3 will get deleted because the one before it is old.
        self.check_one_answer(
            True, [(1, -5, 'upload'), (2, -10, 'upload'), (3, -3, 'upload')],
            ['b2_delete(folder/a, 3, (old version))']
        )

    def check_one_answer(self, has_source, id_relative_date_action_list, expected_actions):
        source_file = LocalSyncPath('a', 'a', 100, 10) if has_source else None
        dest_file_versions = [
            FileVersionInfo(
                id_=id_,
                file_name='folder/' + 'a',
                upload_timestamp=self.today + relative_date * self.one_day_millis,
                action=action,
                size=100,
                file_info={},
                content_type='text/plain',
                content_sha1='content_sha1',
            ) for (id_, relative_date, action) in id_relative_date_action_list
        ]
        dest_file = B2SyncPath(
            'a', selected_version=dest_file_versions[0], all_versions=dest_file_versions
        ) if dest_file_versions else None
        bucket = MagicMock()
        api = MagicMock()
        api.get_bucket_by_name.return_value = bucket
        dest_folder = B2Folder('bucket-1', 'folder', api)
        actual_actions = list(
            make_b2_keep_days_actions(
                source_file, dest_file, dest_folder, dest_folder, self.keep_days, self.today
            )
        )
        actual_action_strs = [str(a) for a in actual_actions]
        self.assertEqual(expected_actions, actual_action_strs)
