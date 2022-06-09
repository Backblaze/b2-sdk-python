######################################################################
#
# File: test/unit/replication/test_monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from apiver_deps import EncryptionAlgorithm, EncryptionKey, EncryptionMode, EncryptionSetting, FileRetentionSetting, ReplicationMonitor, ReplicationRule, ReplicationStatus, RetentionMode, RetentionPeriod, SourceAndDestinationFileAttrs, SourceFileAttrs


SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'some_key', key_id='some-id'),
)


RETENTION_GOVERNANCE = FileRetentionSetting(RetentionMode.GOVERNANCE, retain_until=1)


# def test_error_on_bucket_wo_replication(source_bucket):
#     with pytest.raises(ValueError, 'has no replication configuration'):
#         ReplicationMonitor(source_bucket, rule=ReplicationRule())


# def test_error_when_rule_not_from_replication(source_bucket):
#     source_bucket.replication_configuration = ReplicationConfiguration()
#     with pytest.raises(ValueError, 'is not a rule from'):
#         ReplicationMonitor(source_bucket, rule=ReplicationRule())


def test_iter_pairs(source_bucket, destination_bucket, test_file, monitor):

    source_file = source_bucket.upload_local_file(test_file, 'folder/test.txt')
    source_subfolder_file = source_bucket.upload_local_file(test_file, 'folder/subfolder/test.txt')

    destination_subfolder_file = destination_bucket.upload_local_file(test_file, 'folder/subfolder/test.txt')
    destination_other_file = destination_bucket.upload_local_file(test_file, 'folder/subfolder/test2.txt')

    pairs = [(
        source_path and 'folder/' + source_path.relative_path,
        destination_path and 'folder/' + destination_path.relative_path,
    ) for source_path, destination_path in monitor.iter_pairs()]

    assert set(pairs) == {
        (source_file.file_name, None),
        (source_subfolder_file.file_name, destination_subfolder_file.file_name),
        (None, destination_other_file.file_name),
    }


def test_scan_source(source_bucket, test_file, monitor):
    # upload various types of files to source and get a report
    files = [
        source_bucket.upload_local_file(test_file, 'folder/test-1.txt'),
        source_bucket.upload_local_file(test_file, 'folder/test-2.txt'),
        source_bucket.upload_local_file(test_file, 'not-in-folder.txt'),  # monitor should ignore this
        source_bucket.upload_local_file(test_file, 'folder/test-3.txt', encryption=SSE_C_AES),
        source_bucket.upload_local_file(test_file, 'folder/subfolder/test-4.txt', encryption=SSE_C_AES, file_retention=RETENTION_GOVERNANCE),
        # file_retention: Optional[FileRetentionSetting] = None,
        # legal_hold: Optional[LegalHold] = None,
    ]
    report = monitor.scan_source()
    assert set(report.counter_by_status.items()) == {
        (
            SourceFileAttrs(
                replication_status=None,
                has_hide_marker=True,
                has_sse_c_enabled=False,
                has_large_metadata=False,
                has_file_retention=False,
                has_legal_hold=False,
            ), 2
        ),
        (
            SourceFileAttrs(
                replication_status=None,
                has_hide_marker=True,
                has_sse_c_enabled=True,
                has_large_metadata=False,
                has_file_retention=False,
                has_legal_hold=False,
            ), 1
        ),
        (
            SourceFileAttrs(
                replication_status=None,
                has_hide_marker=True,
                has_sse_c_enabled=True,
                has_large_metadata=False,
                has_file_retention=True,
                has_legal_hold=False,
            ), 1
        ),
    }

    assert report.samples_by_status_first[SourceFileAttrs(
        replication_status=None,
        has_hide_marker=True,
        has_sse_c_enabled=False,
        has_large_metadata=False,
        has_file_retention=False,
        has_legal_hold=False,
    )][0] == files[0]

    assert report.samples_by_status_last[SourceFileAttrs(
        replication_status=None,
        has_hide_marker=True,
        has_sse_c_enabled=False,
        has_large_metadata=False,
        has_file_retention=False,
        has_legal_hold=False,
    )][0] == files[1]

    assert report.samples_by_status_last[SourceFileAttrs(
        replication_status=None,
        has_hide_marker=True,
        has_sse_c_enabled=True,
        has_large_metadata=False,
        has_file_retention=True,
        has_legal_hold=False,
    )][0] == files[4]


def test_scan_source_and_destination():
    raise NotImplementedError()
