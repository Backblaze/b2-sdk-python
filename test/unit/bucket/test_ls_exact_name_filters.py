######################################################################
#
# File: test/unit/bucket/test_ls_exact_name_filters.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest
from apiver_deps import Filter


@pytest.mark.apiver(3)
def test_exact_file_name_honors_exclude_filter(bucket):
    bucket.upload_bytes(b'contents', 'reports/result.txt')
    assert [item.file_name for item, _ in bucket.ls('reports/result.txt')] == ['reports/result.txt']
    assert (
        list(bucket.ls('reports/result.txt', filters=[Filter.exclude('reports/result.txt')])) == []
    )


@pytest.fixture
def ls_exact(bucket, apiver_int):
    def list_exact(path, *, latest_only=True, **kwargs):
        if apiver_int < 3:
            kwargs['folder_to_list_can_be_a_file'] = True
        if apiver_int < 2:
            kwargs['show_versions'] = not latest_only
        else:
            kwargs['latest_only'] = latest_only
        return bucket.ls(path, **kwargs)

    return list_exact


@pytest.mark.apiver(from_ver=1)
@pytest.mark.parametrize(
    'filters, included',
    [
        ([], True),
        ([Filter.exclude('reports/*')], False),
        ([Filter.include('other/*')], False),
        ([Filter.include('reports/*')], True),
        ([Filter.exclude('reports/*'), Filter.include('reports/result.txt')], True),
        ([Filter.include('reports/*'), Filter.exclude('reports/result.txt')], False),
    ],
    ids=['no-filter', 'exclude', 'include-other', 'include', 'last-includes', 'last-excludes'],
)
def test_exact_file_name_uses_filter_order(bucket, ls_exact, filters, included):
    bucket.upload_bytes(b'contents', 'reports/result.txt')
    names = [item.file_name for item, _ in ls_exact('reports/result.txt', filters=filters)]
    assert names == (['reports/result.txt'] if included else [])


@pytest.mark.apiver(from_ver=1)
def test_excluded_exact_file_does_not_fall_back_to_a_folder(bucket, ls_exact):
    bucket.upload_bytes(b'file', 'item')
    bucket.upload_bytes(b'child', 'item/child.txt')
    assert list(ls_exact('item', recursive=True, filters=[Filter.exclude('item')])) == []


@pytest.mark.apiver(from_ver=1)
@pytest.mark.parametrize('latest_only', [True, False])
def test_exact_file_name_filter_applies_to_all_versions(bucket, ls_exact, latest_only):
    bucket.upload_bytes(b'first', 'reports/result.txt')
    bucket.upload_bytes(b'second', 'reports/result.txt')
    all_versions = list(ls_exact('reports/result.txt', latest_only=latest_only))
    assert len(all_versions) == (1 if latest_only else 2)
    assert (
        list(
            ls_exact(
                'reports/result.txt',
                latest_only=latest_only,
                filters=[Filter.exclude('reports/result.txt')],
            )
        )
        == []
    )


@pytest.mark.apiver(0)
@pytest.mark.parametrize('show_versions', [True, False])
def test_v0_excluded_exact_name(bucket, show_versions):
    bucket.upload_bytes(b'contents', 'reports/result.txt')
    assert (
        list(
            bucket.ls(
                'reports/result.txt',
                show_versions=show_versions,
                folder_to_list_can_be_a_file=True,
                filters=[Filter.exclude('reports/result.txt')],
            )
        )
        == []
    )
