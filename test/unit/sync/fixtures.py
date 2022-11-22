######################################################################
#
# File: test/unit/sync/fixtures.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import DEFAULT_SCAN_MANAGER, POLICY_MANAGER, CompareVersionMode, KeepOrDeleteMode, NewerFileSyncMode, Synchronizer


@pytest.fixture(scope='session')
def synchronizer_factory():
    def get_synchronizer(
        policies_manager=DEFAULT_SCAN_MANAGER,
        dry_run=False,
        allow_empty_source=False,
        newer_file_mode=NewerFileSyncMode.RAISE_ERROR,
        keep_days_or_delete=KeepOrDeleteMode.NO_DELETE,
        keep_days=None,
        compare_version_mode=CompareVersionMode.MODTIME,
        compare_threshold=None,
        sync_policy_manager=POLICY_MANAGER,
    ):
        return Synchronizer(
            1,
            policies_manager=policies_manager,
            dry_run=dry_run,
            allow_empty_source=allow_empty_source,
            newer_file_mode=newer_file_mode,
            keep_days_or_delete=keep_days_or_delete,
            keep_days=keep_days,
            compare_version_mode=compare_version_mode,
            compare_threshold=compare_threshold,
            sync_policy_manager=sync_policy_manager,
        )

    return get_synchronizer


@pytest.fixture
def synchronizer(synchronizer_factory):
    return synchronizer_factory()
