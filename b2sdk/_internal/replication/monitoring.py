######################################################################
#
# File: b2sdk/_internal/replication/monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Queue
from typing import ClassVar, Iterator

from ..api import B2Api
from ..bucket import Bucket
from ..encryption.setting import EncryptionMode
from ..file_lock import NO_RETENTION_FILE_SETTING, LegalHold
from ..scan.folder import B2Folder
from ..scan.path import B2Path
from ..scan.policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from ..scan.report import ProgressReport
from ..scan.scan import (
    AbstractScanReport,
    AbstractScanResult,
    CountAndSampleScanReport,
    zip_folders,
)
from .setting import ReplicationRule
from .types import ReplicationStatus


@dataclass(frozen=True)
class ReplicationScanResult(AbstractScanResult):
    """
    Some attributes of source and destination files and their relations
    which are meaningful for replication monitoring and troubleshooting.

    Please be aware that only latest file versions are inspected, so any
    previous file versions are not represented in these results.
    """

    # source attrs
    source_replication_status: ReplicationStatus | None = None
    source_has_hide_marker: bool | None = None
    source_encryption_mode: EncryptionMode | None = None
    source_has_large_metadata: bool | None = None
    source_has_file_retention: bool | None = None
    source_has_legal_hold: bool | None = None

    # destination attrs
    destination_replication_status: ReplicationStatus | None = None

    # source & destination relation attrs
    metadata_differs: bool | None = None
    hash_differs: bool | None = None

    LARGE_METADATA_SIZE: ClassVar[int] = 2048

    @classmethod
    def from_files(
        cls, source_file: B2Path | None = None, destination_file: B2Path | None = None
    ) -> ReplicationScanResult:
        params = {}

        if source_file:
            source_file_version = source_file.selected_version
            params.update(
                {
                    'source_replication_status':
                        source_file_version.replication_status,
                    'source_has_hide_marker':
                        not source_file.is_visible(),
                    'source_encryption_mode':
                        source_file_version.server_side_encryption.mode,
                    'source_has_large_metadata':
                        source_file_version.has_large_header,
                    'source_has_file_retention':
                        source_file_version.file_retention is not NO_RETENTION_FILE_SETTING,
                    'source_has_legal_hold':
                        source_file_version.legal_hold is LegalHold.ON,
                }
            )

        if destination_file:
            params.update(
                {
                    'destination_replication_status':
                        destination_file.selected_version.replication_status,
                }
            )

        if source_file and destination_file:
            source_version = source_file.selected_version
            destination_version = destination_file.selected_version

            params.update(
                {
                    'metadata_differs':
                        source_version.file_info != destination_version.file_info,
                    'hash_differs':
                        (source_version.content_md5 != destination_version.content_md5) or
                        (source_version.content_sha1 != destination_version.content_sha1)
                }
            )

        return cls(**params)


@dataclass
class ReplicationReport(CountAndSampleScanReport):
    SCAN_RESULT_CLASS = ReplicationScanResult


@dataclass
class ReplicationMonitor:
    """
    Calculates source and (optionally) destination replication statistics.

    :param b2sdk.v2.Bucket bucket: replication source bucket
    :param b2sdk.v2.ReplicationRule rule: replication rule to be monitored;
    should belong to `bucket`'s replication configuration
    :param b2sdk.v2.B2Api destination_api: B2Api instance for destination
    bucket; if destination bucket is on the same account as source bucket,
    omit this parameter and then source bucket's B2Api will be used
    :param b2sdk.v2.ProgressReport report: instance of ProgressReport which will report
    scanning progress, by default to stdout
    :param b2sdk.v2.ScanPoliciesManager scan_policies_manager: a strategy to scan
    files, so that several files that match some criteria may be omitted
    :rtype: b2sdk.v2.ReplicationMonitor
    """

    bucket: Bucket
    rule: ReplicationRule
    destination_api: B2Api | None = None  # if None -> will use `api` of source (bucket)
    report: ProgressReport = field(default_factory=lambda: ProgressReport(sys.stdout, False))
    scan_policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER

    REPORT_CLASS: ClassVar[AbstractScanReport] = ReplicationReport
    B2_FOLDER_CLASS: ClassVar[type] = B2Folder
    QUEUE_SIZE: ClassVar[int] = 20_000

    def __post_init__(self):
        if not self.bucket.replication:
            raise ValueError(f'Bucket {self.bucket} has no replication configuration')

        if self.rule not in self.bucket.replication.rules:
            raise ValueError(f'Rule {self.rule} is not a rule from {self.bucket}')

    @property
    def source_api(self) -> B2Api:
        return self.bucket.api

    @property
    def source_folder(self) -> B2_FOLDER_CLASS:
        return self.B2_FOLDER_CLASS(
            bucket_name=self.bucket.name,
            folder_name=self.rule.file_name_prefix,
            api=self.source_api,
        )

    @property
    def destination_bucket(self) -> Bucket:
        destination_api = self.destination_api or self.source_api
        bucket_id = self.rule.destination_bucket_id
        return destination_api.get_bucket_by_id(bucket_id)

    @property
    def destination_folder(self) -> B2_FOLDER_CLASS:
        destination_bucket = self.destination_bucket
        return self.B2_FOLDER_CLASS(
            bucket_name=destination_bucket.name,
            folder_name=self.rule.file_name_prefix,
            api=destination_bucket.api,
        )

    def iter_pairs(self) -> Iterator[tuple[B2Path | None, B2Path | None]]:
        """
        Iterate over files in source and destination and yield pairs.
        Required for replication inspection in-depth.

        Return pair of (source B2Path, destination B2Path). Source or destination
        path may be missing if there's no corresponding destination/source file.
        """
        yield from zip_folders(
            self.source_folder,
            self.destination_folder,
            reporter=self.report,
            policies_manager=self.scan_policies_manager,
        )

    def scan(self, scan_destination: bool = True) -> AbstractScanReport:
        """
        Scan source bucket (only, or with destination) and return replication report.

        No destination scan may give limited replication information, since it only
        checks files on the source bucket without checking whether
        they we really replicated to destination. It may be handy though
        if there is no access to replication destination.
        """
        report = self.REPORT_CLASS()
        queue = Queue(maxsize=self.QUEUE_SIZE)

        if not scan_destination:

            def fill_queue():
                for path in self.source_folder.all_files(
                    policies_manager=self.scan_policies_manager,
                    reporter=self.report,
                ):
                    queue.put((path,), block=True)
                queue.put(None, block=True)
        else:

            def fill_queue():
                for pair in self.iter_pairs():
                    queue.put(pair, block=True)
                queue.put(None, block=True)

        def consume_queue():
            while True:
                items = queue.get(block=True)
                if items is None:  # using None as "end of queue" marker
                    break
                report.add(*items)

        with ThreadPoolExecutor(max_workers=2) as thread_pool:
            futures = [
                thread_pool.submit(fill_queue),
                thread_pool.submit(consume_queue),
            ]

            for future in futures:
                future.result()

        return report
