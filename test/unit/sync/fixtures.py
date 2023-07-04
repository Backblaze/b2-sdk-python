######################################################################
#
# File: test/unit/sync/fixtures.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import apiver_deps
import pytest
from apiver_deps import (
    DEFAULT_SCAN_MANAGER,
    POLICY_MANAGER,
    CompareVersionMode,
    KeepOrDeleteMode,
    NewerFileSyncMode,
    Synchronizer,
    UploadMode,
)


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
        upload_mode=UploadMode.FULL,
        absolute_minimum_part_size=None,
    ):
        kwargs = {}
        if apiver_deps.V < 2:
            assert upload_mode == UploadMode.FULL, "upload_mode not supported in apiver < 2"
            assert absolute_minimum_part_size is None, "absolute_minimum_part_size not supported in apiver < 2"
        else:
            kwargs = dict(
                upload_mode=upload_mode,
                absolute_minimum_part_size=absolute_minimum_part_size,
            )

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
            **kwargs
        )

    return get_synchronizer


@pytest.fixture
def synchronizer(synchronizer_factory):
    return synchronizer_factory()
