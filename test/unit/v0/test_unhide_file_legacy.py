######################################################################
#
# File: test/unit/v0/test_unhide_file_legacy.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


import pytest

from b2sdk._internal.exception import FileNotHidden, FileNotPresent


def test_v0_unhide_removes_hide_marker_and_returns_its_identity(bucket):
    uploaded = bucket.upload_bytes(b'hello', 'example.txt')
    marker = bucket.hide_file('example.txt')

    result = bucket.unhide_file('example.txt')

    assert result.file_id == marker.id_
    assert result.file_name == 'example.txt'
    assert [(item.id_, item.action) for item, _ in bucket.ls(show_versions=True)] == [
        (uploaded.id_, 'upload')
    ]


def test_v0_unhide_missing_exact_name_leaves_matching_prefix_untouched(bucket):
    bucket.upload_bytes(b'other', 'example.txt-extra')
    marker = bucket.hide_file('example.txt-extra')
    with pytest.raises(FileNotPresent):
        bucket.unhide_file('example.txt')
    assert bucket.list_file_versions(prefix='example.txt-extra')['files'][0]['fileId'] == marker.id_


def test_v0_unhide_visible_file_raises_without_deleting_it(bucket):
    uploaded = bucket.upload_bytes(b'hello', 'example.txt')
    with pytest.raises(FileNotHidden):
        bucket.unhide_file('example.txt')
    assert bucket.list_file_versions(prefix='example.txt')['files'][0]['fileId'] == uploaded.id_


def test_v0_unhide_removes_only_the_latest_hide_marker(bucket):
    uploaded = bucket.upload_bytes(b'hello', 'example.txt')
    previous_marker = bucket.hide_file('example.txt')
    latest_marker = bucket.hide_file('example.txt')

    result = bucket.unhide_file('example.txt')

    assert result.file_id == latest_marker.id_
    assert [(item.id_, item.action) for item, _ in bucket.ls(show_versions=True)] == [
        (previous_marker.id_, 'hide'),
        (uploaded.id_, 'upload'),
    ]


def test_v0_unhide_forwards_governance_flag_to_real_session(bucket, mocker):
    bucket.upload_bytes(b'hello', 'example.txt')
    marker = bucket.hide_file('example.txt')
    delete = mocker.spy(bucket.api.session, 'delete_file_version')

    result = bucket.unhide_file('example.txt', bypass_governance=True)

    assert result.file_id == marker.id_
    delete.assert_called_once_with(marker.id_, 'example.txt', True)


def test_v0_list_file_versions_keeps_legacy_raw_response(bucket):
    uploaded = bucket.upload_bytes(b'hello', 'example.txt')
    result = bucket.list_file_versions(start_filename='example.txt', max_entries=1)
    assert isinstance(result, dict)
    assert result['files'][0]['fileId'] == uploaded.id_
    assert 'nextFileName' in result and 'nextFileId' in result
