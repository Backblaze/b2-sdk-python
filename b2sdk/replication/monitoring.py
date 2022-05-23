######################################################################
#
# File: b2sdk/replication/monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from collections import Counter
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple

from ..api import B2Api
from ..bucket import Bucket
from ..scan.folder import B2Folder
from ..scan.report import Report
from ..scan.scan import zip_folders
from .scan.policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from .setting import ReplicationRule
from .types import ReplicationStatus


class ExtendedReplicationStatus(ReplicationStatus):
    """
    FileVersion's ReplicationStatus, but with some statuses
    which may be valuable for replication monitoring.
    """
    HAS_HIDDEN_MARKER = 'HAS_HIDDEN_MARKER'
    HAS_SSE_C_ENABLED = 'HAS_SSE_C_ENABLED'
    HAS_LARGE_METADATA = 'HAS_LARGE_METADATA'


class ReplicationCounter(Counter):
    """
    Counter for accumulating number of files / bytes per ExtendedReplicationStatus.
    """

    @property
    def total(self) -> int:
        """
        Total count. Since one file may have multiple ExtendedReplicationStatusess,
        this property is handy for counting real total count without duplicates.
        """
        return sum(count for status, count in self.items() if status in ReplicationStatus)


def count_files_and_bytes(bucket: Bucket) -> Tuple[ReplicationCounter, ReplicationCounter]:
    """
    Calculate (counter_files, counter_bytes), where each counter
    maps ExtendedReplicationStatuses to number of occurrences.
    """
    counter_files = ReplicationCounter()
    counter_bytes = ReplicationCounter()

    for file_version in bucket.list_file_versions():
        counter_files[file_version.status] += 1
        counter_bytes[file_version.status] += file_version.size

        if file_version.status == ReplicationStatus.COMPLETED:
            if file_version.has_hidden_marker:
                counter_files[ExtendedReplicationStatus.HAS_HIDDEN_MARKER] += 1
            if file_version.has_sse_c_enabled:
                counter_files[ExtendedReplicationStatus.HAS_SSE_C_ENABLED] += 1
            if file_version.has_large_metadata:
                counter_files[ExtendedReplicationStatus.HAS_LARGE_METADATA] += 1

    return counter_files, counter_bytes


@dataclass
class ReplicationMonitor:
    bucket: Bucket
    rule: ReplicationRule
    destination_api: Optional[B2Api] = None  # if None -> will use `api` of source (bucket)

    def __post_init__(self):
        if not self.bucket.replication_configuration:
            raise ValueError(f'Bucket {self.bucket} has no replication configuration')

        if self.rule not in self.bucket.replication_configuration.rules:
            raise ValueError(f'Rule {self.rule} is not a rule from {self.configuration=}')

    @property
    def source_api(self) -> B2Api:
        return self.bucket.api

    # @property
    # def destination_api(self) -> B2Api:
    #     api = B2Api(
    #         account_info=self.source_api.account_info,
    #         cache=self.source_api.cache,
    #     )
    #     api.authorize_account(
    #         realm=self.source_api.account_info.get_realm(),
    #         application_key_id=,
    #         application_key,
    #     )
    #     return api

    @property
    def source_folder(self) -> B2Folder:
        return B2Folder(
            bucket_name=self.bucket.name,
            folder_name=self.rule.file_name_prefix,
            api=self.source_api,
        )

    @property
    def destination_bucket(self) -> Bucket:
        destination_api = self.destination_api or self.source_api
        return destination_api.get_bucket_by_id(
            self.bucket.replication_configuration.destination_bucket_id,
        )

    @property
    def destination_folder(self) -> B2Folder:
        return B2Folder(
            bucket_name=self.destination_bucket.name,
            folder_name='',  # TODO: check this
            api=self.destination_api,
        )

    def iter_diff(
        self,
        report: Optional[Report] = None,
        policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER,
    ) -> Iterator:
        """
        Iterate over files in source and destination and yield pairs that differ.
        Required for replication inspection in-depth.
        """
        yield from zip_folders(
            self.source_folder,
            self.destination_folder,
            report=report or Report(),
            policies_manager=policies_manager,
        )

    def get_source_stats(self) -> Tuple[
        ReplicationCounter[ExtendedReplicationStatus],
        ReplicationCounter[ExtendedReplicationStatus],
    ]:
        return count_files_and_bytes(self.bucket)

    def get_destination_stats(self) -> Tuple[
        ReplicationCounter[ExtendedReplicationStatus],
        ReplicationCounter[ExtendedReplicationStatus],
    ]:
        return count_files_and_bytes(self.destination_bucket)

    @property
    def progress(self) -> Tuple[float, float]:
        source_num_files, source_num_bytes = self.get_source_stats()
        destination_num_files, destination_num_bytes = self.get_destination_stats()
        return (
            destination_num_files.total() / source_num_files.total(),
            destination_num_bytes.total() / source_num_bytes.total(),
        )
