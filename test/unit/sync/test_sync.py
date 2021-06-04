######################################################################
#
# File: test/unit/sync/test_sync.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from collections import defaultdict
from unittest import mock
from enum import Enum
from functools import partial

from apiver_deps import B2DownloadAction, B2UploadAction, B2CopyAction, AbstractSyncEncryptionSettingsProvider, UploadSourceLocalFile
from apiver_deps_exception import DestFileNewer, InvalidArgument
from b2sdk.utils import TempDir

from .fixtures import *

DAY = 86400000  # milliseconds
TODAY = DAY * 100  # an arbitrary reference time for testing


class TestSynchronizer:
    class IllegalEnum(Enum):
        ILLEGAL = 5100

    @pytest.fixture(autouse=True)
    def setup(self, folder_factory, mocker, apiver):
        self.folder_factory = folder_factory
        self.local_folder_factory = partial(folder_factory, 'local')
        self.b2_folder_factory = partial(folder_factory, 'b2')
        self.reporter = mocker.MagicMock()
        self.apiver = apiver

    def _make_folder_sync_actions(self, synchronizer, *args, **kwargs):
        if self.apiver in ['v0', 'v1']:
            return synchronizer.make_folder_sync_actions(*args, **kwargs)
        return synchronizer._make_folder_sync_actions(*args, **kwargs)

    def assert_folder_sync_actions(self, synchronizer, src_folder, dst_folder, expected_actions):
        """
        Checks the actions generated for one file.  The file may or may not
        exist at the source, and may or may not exist at the destination.

        The source and destination files may have multiple versions.
        """

        actions = list(
            self._make_folder_sync_actions(
                synchronizer,
                src_folder,
                dst_folder,
                TODAY,
                self.reporter,
            )
        )
        assert expected_actions == [str(a) for a in actions]

    @pytest.mark.apiver(to_ver=0)
    @pytest.mark.parametrize(
        'args', [
            {
                'newer_file_mode': IllegalEnum.ILLEGAL
            },
            {
                'keep_days_or_delete': IllegalEnum.ILLEGAL
            },
        ],
        ids=[
            'newer_file_mode',
            'keep_days_or_delete',
        ]
    )
    def test_illegal_args_up_to_v0(self, synchronizer_factory, apiver, args):
        from apiver_deps_exception import CommandError
        with pytest.raises(CommandError):
            synchronizer_factory(**args)

    @pytest.mark.apiver(from_ver=1)
    @pytest.mark.parametrize(
        'args', [
            {
                'newer_file_mode': IllegalEnum.ILLEGAL
            },
            {
                'keep_days_or_delete': IllegalEnum.ILLEGAL
            },
        ],
        ids=[
            'newer_file_mode',
            'keep_days_or_delete',
        ]
    )
    def test_illegal_args_up_v1_and_up(self, synchronizer_factory, apiver, args):
        with pytest.raises(InvalidArgument):
            synchronizer_factory(**args)

    def test_illegal(self, synchronizer):
        with pytest.raises(ValueError):
            src = self.local_folder_factory()
            dst = self.local_folder_factory()
            self.assert_folder_sync_actions(synchronizer, src, dst, [])

    # src: absent, dst: absent

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_empty(self, synchronizer, src_type, dst_type):
        src = self.folder_factory(src_type)
        dst = self.folder_factory(dst_type)
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    # # src: present, dst: absent

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_upload(/dir/a.txt, folder/a.txt, 100)']),
            ('b2', 'local', ['b2_download(folder/a.txt, id_a_100, /dir/a.txt, 100)']),
            ('b2', 'b2', ['b2_copy(folder/a.txt, id_a_100, folder/a.txt, 100)']),
        ],
    )
    def test_not_there(self, synchronizer, src_type, dst_type, expected):
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.folder_factory(dst_type)
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,expected',
        [
            ('local', ['b2_upload(/dir/directory/a.txt, folder/directory/a.txt, 100)']),
            ('b2', ['b2_copy(folder/directory/a.txt, id_d_100, folder/directory/a.txt, 100)']),
        ],
    )
    def test_dir_not_there_b2_keepdays(
        self, synchronizer_factory, src_type, expected
    ):  # reproduces issue 220
        src = self.folder_factory(src_type, ('directory/a.txt', [100]))
        dst = self.b2_folder_factory()
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,expected',
        [
            ('local', ['b2_upload(/dir/directory/a.txt, folder/directory/a.txt, 100)']),
            ('b2', ['b2_copy(folder/directory/a.txt, id_d_100, folder/directory/a.txt, 100)']),
        ],
    )
    def test_dir_not_there_b2_delete(
        self, synchronizer_factory, src_type, expected
    ):  # reproduces issue 220
        src = self.folder_factory(src_type, ('directory/a.txt', [100]))
        dst = self.b2_folder_factory()
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    # # src: absent, dst: present

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_no_delete(self, synchronizer, src_type, dst_type):
        src = self.folder_factory(src_type)
        dst = self.folder_factory(dst_type, ('a.txt', [100]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_delete(folder/a.txt, id_a_100, )']),
            ('b2', 'local', ['local_delete(/dir/a.txt)']),
            ('b2', 'b2', ['b2_delete(folder/a.txt, id_a_100, )']),
        ],
    )
    def test_delete(self, synchronizer_factory, src_type, dst_type, expected):
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        src = self.folder_factory(src_type)
        dst = self.folder_factory(dst_type, ('a.txt', [100]))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_delete(folder/a.txt, id_a_100, )']),
            ('b2', 'local', ['local_delete(/dir/a.txt)']),
            ('b2', 'b2', ['b2_delete(folder/a.txt, id_a_100, )']),
        ],
    )
    def test_delete_large(self, synchronizer_factory, src_type, dst_type, expected):
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        src = self.folder_factory(src_type)
        dst = self.folder_factory(dst_type, ('a.txt', [100], 10737418240))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_delete_multiple_versions(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [100, 200]))
        expected = [
            'b2_delete(folder/a.txt, id_a_100, )',
            'b2_delete(folder/a.txt, id_a_200, (old version))'
        ]
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_delete_hide_b2_multiple_versions(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY]))
        expected = [
            'b2_hide(folder/a.txt)', 'b2_delete(folder/a.txt, id_a_8294400000, (old version))'
        ]
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_delete_hide_b2_multiple_versions_old(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=2
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [TODAY - 1 * DAY, TODAY - 3 * DAY, TODAY - 5 * DAY]))
        expected = [
            'b2_hide(folder/a.txt)', 'b2_delete(folder/a.txt, id_a_8208000000, (old version))'
        ]
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_keep(self, synchronizer, src_type):
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [-TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_keep_days(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [-TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY]))
        expected = ['b2_delete(folder/a.txt, id_a_8294400000, (old version))']
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_keep_days_one_old(
        self, synchronizer_factory, src_type
    ):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=5
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(
            ('a.txt', [-(TODAY - 2 * DAY), TODAY - 4 * DAY, TODAY - 6 * DAY])
        )
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_keep_days_two_old(
        self, synchronizer_factory, src_type
    ):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=2
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(
            ('a.txt', [-(TODAY - 2 * DAY), TODAY - 4 * DAY, TODAY - 6 * DAY])
        )
        expected = ['b2_delete(folder/a.txt, id_a_8121600000, (old version))']
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_keep_days_delete_hide_marker(
        self, synchronizer_factory, src_type
    ):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(
            ('a.txt', [-(TODAY - 2 * DAY), TODAY - 4 * DAY, TODAY - 6 * DAY])
        )
        expected = [
            'b2_delete(folder/a.txt, id_a_8467200000, (hide marker))',
            'b2_delete(folder/a.txt, id_a_8294400000, (old version))',
            'b2_delete(folder/a.txt, id_a_8121600000, (old version))'
        ]
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_keep_days_old_delete(
        self, synchronizer_factory, src_type
    ):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [-TODAY + 2 * DAY, TODAY - 4 * DAY]))
        expected = [
            'b2_delete(folder/a.txt, id_a_8467200000, (hide marker))',
            'b2_delete(folder/a.txt, id_a_8294400000, (old version))'
        ]
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_already_hidden_multiple_versions_delete(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        src = self.folder_factory(src_type)
        dst = self.b2_folder_factory(('a.txt', [-TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY]))
        expected = [
            'b2_delete(folder/a.txt, id_a_8640000000, (hide marker))',
            'b2_delete(folder/a.txt, id_a_8467200000, (old version))',
            'b2_delete(folder/a.txt, id_a_8294400000, (old version))'
        ]
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    # # src same as dst

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_same(self, synchronizer, src_type, dst_type):
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.folder_factory(dst_type, ('a.txt', [100]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_same_leave_old_version(self, synchronizer, src_type):
        src = self.folder_factory(src_type, ('a.txt', [TODAY]))
        dst = self.b2_folder_factory(('a.txt', [TODAY, TODAY - 3 * DAY]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_same_clean_old_version(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        src = self.folder_factory(src_type, ('a.txt', [TODAY - 3 * DAY]))
        dst = self.b2_folder_factory(('a.txt', [TODAY - 3 * DAY, TODAY - 4 * DAY]))
        expected = ['b2_delete(folder/a.txt, id_a_8294400000, (old version))']
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_keep_days_no_change_with_old_file(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=1
        )
        src = self.folder_factory(src_type, ('a.txt', [TODAY - 3 * DAY]))
        dst = self.b2_folder_factory(('a.txt', [TODAY - 3 * DAY]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type',
        [
            'local',
            'b2',
        ],
    )
    def test_same_delete_old_versions(self, synchronizer_factory, src_type):
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        src = self.folder_factory(src_type, ('a.txt', [TODAY]))
        dst = self.b2_folder_factory(('a.txt', [TODAY, TODAY - 3 * DAY]))
        expected = ['b2_delete(folder/a.txt, id_a_8380800000, (old version))']
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    # # src newer than dst

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_upload(/dir/a.txt, folder/a.txt, 200)']),
            ('b2', 'local', ['b2_download(folder/a.txt, id_a_200, /dir/a.txt, 200)']),
            ('b2', 'b2', ['b2_copy(folder/a.txt, id_a_200, folder/a.txt, 200)']),
        ],
    )
    def test_never(self, synchronizer, src_type, dst_type, expected):
        src = self.folder_factory(src_type, ('a.txt', [200]))
        dst = self.folder_factory(dst_type, ('a.txt', [100]))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,expected',
        [
            (
                'local', [
                    'b2_upload(/dir/a.txt, folder/a.txt, 8640000000)',
                    'b2_delete(folder/a.txt, id_a_8208000000, (old version))',
                ]
            ),
            (
                'b2', [
                    'b2_copy(folder/a.txt, id_a_8640000000, folder/a.txt, 8640000000)',
                    'b2_delete(folder/a.txt, id_a_8208000000, (old version))',
                ]
            ),
        ],
    )
    def test_newer_clean_old_versions(self, synchronizer_factory, src_type, expected):
        synchronizer = synchronizer_factory(
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE, keep_days=2
        )
        src = self.folder_factory(src_type, ('a.txt', [TODAY]))
        dst = self.b2_folder_factory(('a.txt', [TODAY - 1 * DAY, TODAY - 3 * DAY, TODAY - 5 * DAY]))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,expected',
        [
            (
                'local', [
                    'b2_upload(/dir/a.txt, folder/a.txt, 8640000000)',
                    'b2_delete(folder/a.txt, id_a_8553600000, (old version))',
                    'b2_delete(folder/a.txt, id_a_8380800000, (old version))',
                ]
            ),
            (
                'b2', [
                    'b2_copy(folder/a.txt, id_a_8640000000, folder/a.txt, 8640000000)',
                    'b2_delete(folder/a.txt, id_a_8553600000, (old version))',
                    'b2_delete(folder/a.txt, id_a_8380800000, (old version))',
                ]
            ),
        ],
    )
    def test_newer_delete_old_versions(self, synchronizer_factory, src_type, expected):
        synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
        src = self.folder_factory(src_type, ('a.txt', [TODAY]))
        dst = self.b2_folder_factory(('a.txt', [TODAY - 1 * DAY, TODAY - 3 * DAY]))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    # # src older than dst

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_upload(/dir/a.txt, folder/a.txt, 200)']),
            ('b2', 'local', ['b2_download(folder/a.txt, id_a_200, /dir/a.txt, 200)']),
            ('b2', 'b2', ['b2_copy(folder/a.txt, id_a_200, folder/a.txt, 200)']),
        ],
    )
    def test_older(self, synchronizer, apiver, src_type, dst_type, expected):
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.folder_factory(dst_type, ('a.txt', [200]))
        with pytest.raises(DestFileNewer) as excinfo:
            self.assert_folder_sync_actions(synchronizer, src, dst, expected)
        messages = defaultdict(
            lambda: 'source file is older than destination: %s://a.txt with a time of 100 '
                    'cannot be synced to %s://a.txt with a time of 200, '
                    'unless a valid newer_file_mode is provided',
            v0='source file is older than destination: %s://a.txt with a time of 100 '
               'cannot be synced to %s://a.txt with a time of 200, '
               'unless --skipNewer or --replaceNewer is provided'
        )  # yapf: disable

        assert str(excinfo.value) == messages[apiver] % (src_type, dst_type)

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_older_skip(self, synchronizer_factory, src_type, dst_type):
        synchronizer = synchronizer_factory(newer_file_mode=NewerFileSyncMode.SKIP)
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.folder_factory(dst_type, ('a.txt', [200]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_upload(/dir/a.txt, folder/a.txt, 100)']),
            ('b2', 'local', ['b2_download(folder/a.txt, id_a_100, /dir/a.txt, 100)']),
            ('b2', 'b2', ['b2_copy(folder/a.txt, id_a_100, folder/a.txt, 100)']),
        ],
    )
    def test_older_replace(self, synchronizer_factory, src_type, dst_type, expected):
        synchronizer = synchronizer_factory(newer_file_mode=NewerFileSyncMode.REPLACE)
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.folder_factory(dst_type, ('a.txt', [200]))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,expected',
        [
            (
                'local', [
                    'b2_upload(/dir/a.txt, folder/a.txt, 100)',
                    'b2_delete(folder/a.txt, id_a_200, (old version))',
                ]
            ),
            (
                'b2', [
                    'b2_copy(folder/a.txt, id_a_100, folder/a.txt, 100)',
                    'b2_delete(folder/a.txt, id_a_200, (old version))',
                ]
            ),
        ],
    )
    def test_older_replace_delete(self, synchronizer_factory, src_type, expected):
        synchronizer = synchronizer_factory(
            newer_file_mode=NewerFileSyncMode.REPLACE, keep_days_or_delete=KeepOrDeleteMode.DELETE
        )
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.b2_folder_factory(('a.txt', [200]))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    # # compareVersions option

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_compare_none_newer(self, synchronizer_factory, src_type, dst_type):
        synchronizer = synchronizer_factory(compare_version_mode=CompareVersionMode.NONE)
        src = self.folder_factory(src_type, ('a.txt', [200]))
        dst = self.folder_factory(dst_type, ('a.txt', [100]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_compare_none_older(self, synchronizer_factory, src_type, dst_type):
        synchronizer = synchronizer_factory(compare_version_mode=CompareVersionMode.NONE)
        src = self.folder_factory(src_type, ('a.txt', [100]))
        dst = self.folder_factory(dst_type, ('a.txt', [200]))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type,dst_type',
        [
            ('local', 'b2'),
            ('b2', 'local'),
            ('b2', 'b2'),
        ],
    )
    def test_compare_size_equal(self, synchronizer_factory, src_type, dst_type):
        synchronizer = synchronizer_factory(compare_version_mode=CompareVersionMode.SIZE)
        src = self.folder_factory(src_type, ('a.txt', [200], 10))
        dst = self.folder_factory(dst_type, ('a.txt', [100], 10))
        self.assert_folder_sync_actions(synchronizer, src, dst, [])

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            ('local', 'b2', ['b2_upload(/dir/a.txt, folder/a.txt, 200)']),
            ('b2', 'local', ['b2_download(folder/a.txt, id_a_200, /dir/a.txt, 200)']),
            ('b2', 'b2', ['b2_copy(folder/a.txt, id_a_200, folder/a.txt, 200)']),
        ],
    )
    def test_compare_size_not_equal(self, synchronizer_factory, src_type, dst_type, expected):
        synchronizer = synchronizer_factory(compare_version_mode=CompareVersionMode.SIZE)
        src = self.folder_factory(src_type, ('a.txt', [200], 11))
        dst = self.folder_factory(dst_type, ('a.txt', [100], 10))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    @pytest.mark.parametrize(
        'src_type,dst_type,expected',
        [
            (
                'local', 'b2', [
                    'b2_upload(/dir/a.txt, folder/a.txt, 200)',
                    'b2_delete(folder/a.txt, id_a_100, (old version))'
                ]
            ),
            ('b2', 'local', ['b2_download(folder/a.txt, id_a_200, /dir/a.txt, 200)']),
            (
                'b2', 'b2', [
                    'b2_copy(folder/a.txt, id_a_200, folder/a.txt, 200)',
                    'b2_delete(folder/a.txt, id_a_100, (old version))'
                ]
            ),
        ],
    )
    def test_compare_size_not_equal_delete(
        self, synchronizer_factory, src_type, dst_type, expected
    ):
        synchronizer = synchronizer_factory(
            compare_version_mode=CompareVersionMode.SIZE,
            keep_days_or_delete=KeepOrDeleteMode.DELETE
        )
        src = self.folder_factory(src_type, ('a.txt', [200], 11))
        dst = self.folder_factory(dst_type, ('a.txt', [100], 10))
        self.assert_folder_sync_actions(synchronizer, src, dst, expected)

    # FIXME: rewrite this test to not use mock.call checks when all of Synchronizers tests are rewritten to test_bucket
    # style - i.e. with simulated api and fake files returned from methods. Then, checking EncryptionSetting used for
    # transmission will be done by the underlying simulator.
    def test_encryption_b2_to_local(self, synchronizer_factory, apiver):
        local = self.local_folder_factory()
        remote = self.b2_folder_factory(('directory/b.txt', [100]))
        synchronizer = synchronizer_factory()

        encryption = object()
        bucket = mock.MagicMock()
        provider = TstEncryptionSettingsProvider(encryption, encryption)
        download_action = next(
            iter(
                self._make_folder_sync_actions(
                    synchronizer,
                    remote,
                    local,
                    TODAY,
                    self.reporter,
                    encryption_settings_provider=provider
                )
            )
        )
        with mock.patch.object(B2DownloadAction, '_ensure_directory_existence'):
            try:
                download_action.do_action(bucket, self.reporter)
            except:
                pass

        assert bucket.mock_calls[0] == mock.call.download_file_by_id(
            'id_d_100', progress_listener=mock.ANY, encryption=encryption
        )

        if apiver in ['v0', 'v1']:
            file_version_kwarg = 'file_version_info'
        else:
            file_version_kwarg = 'file_version'

        provider.get_setting_for_download.assert_has_calls(
            [mock.call(
                bucket=bucket,
                **{file_version_kwarg: mock.ANY},
            )]
        )

    # FIXME: rewrite this test to not use mock.call checks when all of Synchronizers tests are rewritten to test_bucket
    # style - i.e. with simulated api and fake files returned from methods. Then, checking EncryptionSetting used for
    # transmission will be done by the underlying simulator.
    def test_encryption_local_to_b2(self, synchronizer_factory):
        local = self.local_folder_factory(('directory/a.txt', [100]))
        remote = self.b2_folder_factory()
        synchronizer = synchronizer_factory()

        encryption = object()
        bucket = mock.MagicMock()
        provider = TstEncryptionSettingsProvider(encryption, encryption)
        upload_action = next(
            iter(
                self._make_folder_sync_actions(
                    synchronizer,
                    local,
                    remote,
                    TODAY,
                    self.reporter,
                    encryption_settings_provider=provider
                )
            )
        )
        with mock.patch.object(UploadSourceLocalFile, 'check_path_and_get_size'):
            try:
                upload_action.do_action(bucket, self.reporter)
            except:
                pass

        assert bucket.mock_calls == [
            mock.call.upload(
                mock.ANY,
                'folder/directory/a.txt',
                file_info={'src_last_modified_millis': '100'},
                progress_listener=mock.ANY,
                encryption=encryption
            )
        ]

        assert provider.get_setting_for_upload.mock_calls == [
            mock.call(
                bucket=bucket,
                b2_file_name='folder/directory/a.txt',
                file_info={'src_last_modified_millis': '100'},
                length=10
            )
        ]

    # FIXME: rewrite this test to not use mock.call checks when all of Synchronizers tests are rewritten to test_bucket
    # style - i.e. with simulated api and fake files returned from methods. Then, checking EncryptionSetting used for
    # transmission will be done by the underlying simulator.
    def test_encryption_b2_to_b2(self, synchronizer_factory, apiver):
        src = self.b2_folder_factory(('directory/a.txt', [100]))
        dst = self.b2_folder_factory()
        synchronizer = synchronizer_factory()

        source_encryption = object()
        destination_encryption = object()
        bucket = mock.MagicMock()
        provider = TstEncryptionSettingsProvider(source_encryption, destination_encryption)
        copy_action = next(
            iter(
                self._make_folder_sync_actions(
                    synchronizer,
                    src,
                    dst,
                    TODAY,
                    self.reporter,
                    encryption_settings_provider=provider
                )
            )
        )
        copy_action.do_action(bucket, self.reporter)

        assert bucket.mock_calls == [
            mock.call.copy(
                'id_d_100',
                'folder/directory/a.txt',
                length=10,
                source_content_type='text/plain',
                source_file_info={'in_b2': 'yes'},
                progress_listener=mock.ANY,
                source_encryption=source_encryption,
                destination_encryption=destination_encryption
            )
        ]

        if apiver in ['v0', 'v1']:
            file_version_kwarg = 'source_file_version_info'
            additional_kwargs = {'target_file_info': None}
        else:
            file_version_kwarg = 'source_file_version'
            additional_kwargs = {}

        assert provider.get_source_setting_for_copy.mock_calls == [
            mock.call(
                bucket=mock.ANY,
                **{file_version_kwarg: mock.ANY},
            )
        ]

        assert provider.get_destination_setting_for_copy.mock_calls == [
            mock.call(
                bucket=mock.ANY,
                dest_b2_file_name='folder/directory/a.txt',
                **additional_kwargs,
                **{file_version_kwarg: mock.ANY},
            )
        ]


class TstEncryptionSettingsProvider(AbstractSyncEncryptionSettingsProvider):
    def __init__(self, source_encryption_setting, destination_encryption_setting):
        self.get_setting_for_upload = mock.MagicMock(
            side_effect=lambda *a, **kw: destination_encryption_setting
        )
        self.get_source_setting_for_copy = mock.MagicMock(
            side_effect=lambda *a, **kw: source_encryption_setting
        )
        self.get_destination_setting_for_copy = mock.MagicMock(
            side_effect=lambda *a, **kw: destination_encryption_setting
        )
        self.get_setting_for_download = mock.MagicMock(
            side_effect=lambda *a, **kw: source_encryption_setting
        )

    def get_setting_for_upload(self, *a, **kw):
        """overwritten in __init__"""

    def get_source_setting_for_copy(self, *a, **kw):
        """overwritten in __init__"""

    def get_destination_setting_for_copy(self, *a, **kw):
        """overwritten in __init__"""

    def get_setting_for_download(self, *a, **kw):
        """overwritten in __init__"""
