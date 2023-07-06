######################################################################
#
# File: test/unit/replication/test_monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from apiver_deps import (
    SSE_B2_AES,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FileRetentionSetting,
    ReplicationScanResult,
    RetentionMode,
)

SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=b'some_key', key_id='some-id'),
)

RETENTION_GOVERNANCE = FileRetentionSetting(RetentionMode.GOVERNANCE, retain_until=1)

DEFAULT_REPLICATION_RESULT = dict(
    source_replication_status=None,
    source_has_hide_marker=False,
    source_encryption_mode=EncryptionMode.NONE,
    source_has_large_metadata=False,
    source_has_file_retention=False,
    source_has_legal_hold=False,
    destination_replication_status=None,
    metadata_differs=None,
    hash_differs=None,
)


def test_iter_pairs(source_bucket, destination_bucket, test_file, monitor):

    source_file = source_bucket.upload_local_file(test_file, 'folder/test.txt')
    source_subfolder_file = source_bucket.upload_local_file(test_file, 'folder/subfolder/test.txt')

    destination_subfolder_file = destination_bucket.upload_local_file(
        test_file, 'folder/subfolder/test.txt'
    )
    destination_other_file = destination_bucket.upload_local_file(
        test_file, 'folder/subfolder/test2.txt'
    )

    pairs = [
        (
            source_path and 'folder/' + source_path.relative_path,
            destination_path and 'folder/' + destination_path.relative_path,
        ) for source_path, destination_path in monitor.iter_pairs()
    ]

    assert set(pairs) == {
        (source_file.file_name, None),
        (source_subfolder_file.file_name, destination_subfolder_file.file_name),
        (None, destination_other_file.file_name),
    }


def test_scan_source(source_bucket, test_file, monitor):
    # upload various types of files to source and get a report
    files = [
        source_bucket.upload_local_file(test_file, 'folder/test-1-1.txt'),
        source_bucket.upload_local_file(test_file, 'folder/test-1-2.txt'),
        source_bucket.upload_local_file(test_file, 'folder/test-2.txt', encryption=SSE_B2_AES),
        source_bucket.upload_local_file(test_file,
                                        'not-in-folder.txt'),  # monitor should ignore this
        source_bucket.upload_local_file(test_file, 'folder/test-3.txt', encryption=SSE_C_AES),
        source_bucket.upload_local_file(test_file, 'folder/test-4.txt', encryption=SSE_C_AES),
        source_bucket.upload_local_file(
            test_file,
            'folder/subfolder/test-5.txt',
            encryption=SSE_C_AES,
            file_retention=RETENTION_GOVERNANCE
        ),
        source_bucket.upload_local_file(
            test_file,
            'folder/test-large-meta.txt',
            file_info={
                'dummy-key': 'a' * 7000,
            },
        ),
        source_bucket.upload_local_file(
            test_file,
            'folder/test-large-meta-encrypted.txt',
            file_info={
                'dummy-key': 'a' * 2048,
            },
            encryption=SSE_C_AES,
        ),
    ]
    report = monitor.scan(scan_destination=False)

    assert report.counter_by_status[ReplicationScanResult(**DEFAULT_REPLICATION_RESULT)] == 2

    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'source_encryption_mode': EncryptionMode.SSE_B2,
        }
    )] == 1

    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'source_encryption_mode': EncryptionMode.SSE_C,
        }
    )] == 2

    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'source_encryption_mode': EncryptionMode.SSE_C,
            'source_has_file_retention': True,
        }
    )] == 1

    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'source_has_large_metadata': True,
        }
    )] == 1

    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'source_encryption_mode': EncryptionMode.SSE_C,
            'source_has_large_metadata': True,
        }
    )] == 1

    # ---- first and last ----

    assert report.samples_by_status_first[ReplicationScanResult(**DEFAULT_REPLICATION_RESULT,)
                                         ][0] == files[0]

    assert report.samples_by_status_last[ReplicationScanResult(**DEFAULT_REPLICATION_RESULT,)
                                        ][0] == files[1]


def test_scan_source_and_destination(
    source_bucket, destination_bucket, test_file, test_file_reversed, monitor
):
    _ = [
        # match
        source_bucket.upload_local_file(test_file, 'folder/test-1.txt'),
        destination_bucket.upload_local_file(test_file, 'folder/test-1.txt'),

        # missing on destination
        source_bucket.upload_local_file(test_file, 'folder/test-2.txt'),

        # missing on source
        destination_bucket.upload_local_file(test_file, 'folder/test-3.txt'),

        # metadata differs
        source_bucket.upload_local_file(
            test_file, 'folder/test-4.txt', file_info={
                'haha': 'hoho',
            }
        ),
        destination_bucket.upload_local_file(
            test_file, 'folder/test-4.txt', file_info={
                'hehe': 'hihi',
            }
        ),

        # hash differs
        source_bucket.upload_local_file(test_file, 'folder/test-5.txt'),
        destination_bucket.upload_local_file(test_file_reversed, 'folder/test-5.txt'),
    ]

    report = monitor.scan(scan_destination=True)

    # match
    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'metadata_differs': False,
            'hash_differs': False,
        }
    )] == 1

    # missing on destination
    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'destination_replication_status': None,
        }
    )] == 1

    # missing on source
    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'source_replication_status': None,
            'source_has_hide_marker': None,
            'source_encryption_mode': None,
            'source_has_large_metadata': None,
            'source_has_file_retention': None,
            'source_has_legal_hold': None,
        }
    )] == 1

    # metadata differs
    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'metadata_differs': True,
            'hash_differs': False,
        }
    )] == 1

    # hash differs
    assert report.counter_by_status[ReplicationScanResult(
        **{
            **DEFAULT_REPLICATION_RESULT,
            'metadata_differs': False,
            'hash_differs': True,
        }
    )] == 1
