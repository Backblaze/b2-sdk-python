######################################################################
#
# File: test/unit/sync/test_excluded_destination_delete.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from unittest import mock

import pytest
from apiver_deps import KeepOrDeleteMode, ScanPoliciesManager

from ..fixtures.folder import *  # noqa: F403
from .fixtures import *  # noqa: F403


@pytest.mark.apiver(from_ver=3)
@pytest.mark.parametrize('src_type,dst_type', [('local', 'b2'), ('b2', 'local'), ('b2', 'b2')])
@pytest.mark.parametrize('source_has_excluded', [False, True])
def test_delete_sync_preserves_excluded_destination_files(
    folder_factory, synchronizer_factory, src_type, dst_type, source_has_excluded
):
    source = folder_factory(src_type, *([('secret.txt', [100])] if source_has_excluded else []))
    destination = folder_factory(dst_type, ('secret.txt', [100]), ('remove.txt', [100]))
    policy = ScanPoliciesManager(exclude_file_regexes=[r'secret\.txt$'])
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source, destination, 1000, mock.MagicMock(), policies_manager=policy
        )
    )
    descriptions = [str(action) for action in actions]
    assert len(descriptions) == 1
    assert 'remove.txt' in descriptions[0]
    assert all('secret.txt' not in item for item in descriptions)


@pytest.mark.apiver(from_ver=3)
@pytest.mark.parametrize('dst_type', ['local', 'b2'])
def test_delete_sync_preserves_excluded_directory_and_honors_include(
    folder_factory, synchronizer_factory, dst_type
):
    source = folder_factory('b2')
    destination = folder_factory(
        dst_type, ('private/nested/secret.txt', [100]), ('keep.txt', [100]), ('remove.txt', [100])
    )
    policy = ScanPoliciesManager(
        exclude_dir_regexes=[r'private$'],
        exclude_file_regexes=[r'.*'],
        include_file_regexes=[r'remove\.txt$'],
    )
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source, destination, 1000, mock.MagicMock(), policies_manager=policy
        )
    )
    descriptions = [str(action) for action in actions]
    assert len(descriptions) == 1
    assert 'remove.txt' in descriptions[0]


@pytest.mark.apiver(from_ver=3)
def test_destination_time_filter_does_not_hide_existing_file(folder_factory, synchronizer_factory):
    source = folder_factory('local', ('a.txt', [200]))
    destination = folder_factory('b2', ('a.txt', [100]))
    policy = ScanPoliciesManager(exclude_modified_before=150)
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source, destination, 1000, mock.MagicMock(), policies_manager=policy
        )
    )
    assert any('id_a_100' in str(action) for action in actions)


@pytest.mark.apiver(from_ver=3)
def test_local_delete_directory_exclusion_uses_posix_separators(
    folder_factory, synchronizer_factory, monkeypatch
):
    from pathlib import PureWindowsPath

    from b2sdk._internal.sync import sync as sync_module

    monkeypatch.setattr(sync_module, 'Path', PureWindowsPath, raising=False)
    source = folder_factory('b2')
    destination = folder_factory(
        'local', ('private/nested/secret.txt', [100]), ('remove.txt', [100])
    )
    policy = ScanPoliciesManager(exclude_dir_regexes=[r'private/nested$'])
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source, destination, 1000, mock.MagicMock(), policies_manager=policy
        )
    )
    assert len(actions) == 1
    assert 'remove.txt' in str(actions[0])


@pytest.mark.apiver(from_ver=3)
@pytest.mark.parametrize('dst_type', ['local', 'b2'])
def test_delete_sync_directory_only_exclusion(folder_factory, synchronizer_factory, dst_type):
    source = folder_factory('b2')
    destination = folder_factory(
        dst_type, ('private/nested/secret.txt', [100]), ('remove.txt', [100])
    )
    policy = ScanPoliciesManager(exclude_dir_regexes=[r'private$'])
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source, destination, 1000, mock.MagicMock(), policies_manager=policy
        )
    )
    assert len(actions) == 1
    assert 'remove.txt' in str(actions[0])


@pytest.mark.apiver(from_ver=3)
@pytest.mark.parametrize(
    'policy_kwargs,excluded_time,included_time',
    [({'exclude_modified_before': 150}, 100, 200), ({'exclude_modified_after': 150}, 200, 100)],
    ids=['before', 'after'],
)
def test_local_destination_only_respects_modified_time(
    folder_factory, synchronizer_factory, policy_kwargs, excluded_time, included_time
):
    source = folder_factory('b2')
    destination = folder_factory(
        'local', ('excluded.txt', [excluded_time]), ('remove.txt', [included_time])
    )
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source,
            destination,
            1000,
            mock.MagicMock(),
            policies_manager=ScanPoliciesManager(**policy_kwargs),
        )
    )
    assert len(actions) == 1
    assert 'remove.txt' in str(actions[0])


@pytest.mark.apiver(from_ver=3)
@pytest.mark.parametrize(
    'policy_kwargs,excluded_time,included_time',
    [
        ({'exclude_modified_before': 150}, 100, 200),
        ({'exclude_modified_after': 150}, 200, 100),
        ({'exclude_uploaded_before': 150}, 100, 200),
        ({'exclude_uploaded_after': 150}, 200, 100),
    ],
    ids=['modified-before', 'modified-after', 'uploaded-before', 'uploaded-after'],
)
def test_b2_destination_only_respects_time(
    folder_factory, synchronizer_factory, policy_kwargs, excluded_time, included_time
):
    source = folder_factory('local')
    destination = folder_factory(
        'b2', ('excluded.txt', [excluded_time]), ('remove.txt', [included_time])
    )
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source,
            destination,
            1000,
            mock.MagicMock(),
            policies_manager=ScanPoliciesManager(**policy_kwargs),
        )
    )
    assert len(actions) == 1
    assert 'remove.txt' in str(actions[0])


@pytest.mark.apiver(from_ver=3)
def test_b2_destination_only_preserves_time_excluded_older_version(
    folder_factory, synchronizer_factory
):
    source = folder_factory('local')
    destination = folder_factory('b2', ('file.txt', [200, 100]))
    policy = ScanPoliciesManager(exclude_modified_before=150)
    synchronizer = synchronizer_factory(keep_days_or_delete=KeepOrDeleteMode.DELETE)
    actions = list(
        synchronizer._make_folder_sync_actions(
            source, destination, 1000, mock.MagicMock(), policies_manager=policy
        )
    )
    assert len(actions) == 1
    assert 'id_f_200' in str(actions[0])
