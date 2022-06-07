######################################################################
#
# File: test/unit/replication/monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.replication.monitoring import ReplicationMonitor
from b2sdk.replication.setting import ReplicationConfiguration, ReplicationRule


# def test_error_on_bucket_wo_replication(source_bucket):
#     with pytest.raises(ValueError, 'has no replication configuration'):
#         ReplicationMonitor(source_bucket, rule=ReplicationRule())


# def test_error_when_rule_not_from_replication(source_bucket):
#     source_bucket.replication_configuration = ReplicationConfiguration()
#     with pytest.raises(ValueError, 'is not a rule from'):
#         ReplicationMonitor(source_bucket, rule=ReplicationRule())


def test_iter_pairs(source_bucket, destination_bucket, test_file):
    source_bucket.replication_configuration = ReplicationConfiguration(
        rules=[
            ReplicationRule(
                destination_bucket_id=destination_bucket.id_,
                name='name',
                file_name_prefix='folder/',  # TODO: is last slash needed?
            ),
        ],
        source_key_id='hoho|trololo',
    )

    monitor = ReplicationMonitor(source_bucket, rule=source_bucket.replication_configuration.rules[0])

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


def test_scan_source():
    raise NotImplementedError()


def test_scan_source_and_destination():
    raise NotImplementedError()
