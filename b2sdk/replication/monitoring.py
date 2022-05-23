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
from .types import CompletedReplicationStatus, ReplicationStatus


@dataclass
class FilesBytesCounter:
    def __post_init__(self):
        self._files = Counter()
        self._bytes = Counter()

    @property
    def files(self) -> Counter:
        return self._files

    @property
    def bytes(self) -> Counter:
        return self._bytes

    @property
    def total_files(self) -> int:
        return sum(self._files.values())

    @property
    def total_bytes(self) -> int:
        return sum(self._bytes.values())


def count_files_and_bytes(bucket: Bucket) -> Tuple[FilesBytesCounter, FilesBytesCounter]:
    counter, counter_completed = FilesBytesCounter(), FilesBytesCounter()

    for file_version in bucket.list_file_versions():
        counter.files[file_version.status] += 1
        file_size = file_version.size
        counter.bytes[file_version.status] += file_size

        if file_version.status == ReplicationStatus.COMPLETED:
            if file_version.has_hidden_marker:
                counter_completed.files[CompletedReplicationStatus.HAS_HIDDEN_MARKER] += 1
                counter_completed.bytes[CompletedReplicationStatus.HAS_HIDDEN_MARKER] += file_size
            if file_version.has_sse_c_enabled:
                counter_completed.files[CompletedReplicationStatus.HAS_SSE_C_ENABLED] += 1
                counter_completed.bytes[CompletedReplicationStatus.HAS_SSE_C_ENABLED] += file_size
            if file_version.has_large_metadata:
                counter_completed.files[CompletedReplicationStatus.HAS_LARGE_METADATA] += 1
                counter_completed.bytes[CompletedReplicationStatus.HAS_LARGE_METADATA] += file_size

    return counter, counter_completed


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

    def get_source_stats(self) -> Tuple[FilesBytesCounter, FilesBytesCounter]:
        return count_files_and_bytes(self.bucket)

    def get_destination_stats(self) -> Tuple[FilesBytesCounter, FilesBytesCounter]:
        return count_files_and_bytes(self.destination_bucket)

    @property
    def progress(self) -> Tuple[float, float]:  # (files, bytes)
        source_counter, _ = self.get_source_stats()
        destination_counter, _ = self.get_destination_stats()
        return (
            destination_counter.total_files() / source_counter.total_files(),
            destination_counter.total_bytes() / source_counter.total_bytes(),
        )
