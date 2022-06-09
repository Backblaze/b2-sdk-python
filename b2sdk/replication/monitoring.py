######################################################################
#
# File: b2sdk/replication/monitoring.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys

from collections import Counter
from dataclasses import dataclass, field
from typing import ClassVar, Dict, Iterator, Optional, Tuple, Type, Union

from ..api import B2Api
from ..bucket import Bucket
from ..encryption.setting import EncryptionMode
from ..file_lock import NO_RETENTION_FILE_SETTING, LegalHold
from ..file_version import FileVersion
from ..scan.folder import B2Folder
from ..scan.path import B2Path
from ..scan.policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from ..scan.report import Report
from ..scan.scan import zip_folders
from .setting import ReplicationRule
from .types import ReplicationStatus


class FileAttrs:
    pass


@dataclass(frozen=True)
class SourceFileAttrs(FileAttrs):
    """
    Some attributes of source files which are meaningful
    for replication monitoring and troubleshooting.
    """
    replication_status: ReplicationStatus
    has_hide_marker: bool
    has_sse_c_enabled: bool
    has_large_metadata: bool
    has_file_retention: bool
    has_legal_hold: bool

    LARGE_METADATA_SIZE: ClassVar[int] = 2048

    @classmethod
    def from_file(cls, file: B2Path) -> 'SourceFileAttrs':
        file_version = file.selected_version
        return cls(
            replication_status=file_version.replication_status,
            has_hide_marker=file.is_visible(),
            has_sse_c_enabled=file_version.server_side_encryption.mode == EncryptionMode.SSE_C,
            has_large_metadata=file_version.headers_size >= cls.LARGE_METADATA_SIZE,
            has_file_retention=file_version.file_retention is not NO_RETENTION_FILE_SETTING,
            has_legal_hold=file_version.legal_hold is not LegalHold.UNSET,
        )


@dataclass(frozen=True)
class SourceAndDestinationFileAttrs(FileAttrs):
    """
    Some attributes of source and destination files and their relations
    which are meaningful for replication monitoring and troubleshooting.
    """
    source: SourceFileAttrs
    destination_replication_status: ReplicationStatus
    metadata_differs: bool
    # TODO: more features

    @classmethod
    def from_files(cls, source_file: B2Path, destination_file: B2Path) -> 'SourceAndDestinationFileAttrs':
        destination_version = destination_file.selected_version
        return cls(
            source_replication_status=SourceAndDestinationFileAttrs.from_file(source_file),
            destination_replication_status=destination_version.replication_status,
            metadata_differs=None,  # TODO
        )


@dataclass
class ReplicationReport:
    """
    Aggregation of valuable information about file replication
    after scanning source and (optionally) destination folders.
    """

    def add(self, source_file: B2Path, destination_file: Optional[B2Path]):
        raise NotImplementedError()


@dataclass
class CountAndSampleReplicationReport(ReplicationReport):
    """
    Replication report which groups and counts files by their `FileAttrs` and
    also stores first and last seen examples of such files.
    """
    counter_by_status: Counter[FileAttrs] = field(default_factory=Counter)
    samples_by_status_first: Dict[FileAttrs, Tuple[FileVersion, FileVersion]] = field(default_factory=dict)
    samples_by_status_last: Dict[FileAttrs, Tuple[FileVersion, FileVersion]] = field(default_factory=dict)

    def add(self, source_file: B2Path, destination_file: Optional[B2Path] = None):
        if destination_file:
            status = SourceAndDestinationFileAttrs.from_files(source_file, destination_file)
        else:
            status = SourceFileAttrs.from_file(source_file)
        self.counter_by_status[status] += 1

        sample = (
            source_file.selected_version,
            destination_file and destination_file.selected_version,
        )
        if status not in self.samples_by_status_first:
            self.samples_by_status_first[status] = sample
        self.samples_by_status_last[status] = sample


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
    :param type replication_report_class: subclass of ReplicationReport,
    used to aggregate files to statistics
    :param b2sdk.v2.Report report: instance of Report which will report
    scanning progress, by default to stdout
    :param b2sdk.v2.ScanPoliciesManager scan_policies_manager: a strategy to scan
    files, so that several files that match some criteria may be omitted
    :rtype: b2sdk.v2.ReplicationMonitor
    """

    bucket: Bucket
    rule: ReplicationRule
    destination_api: Optional[B2Api] = None  # if None -> will use `api` of source (bucket)
    replication_report_class: Type[ReplicationReport] = CountAndSampleReplicationReport
    report: Report = field(default_factory=lambda: Report(sys.stdout, False))
    scan_policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER

    B2_FOLDER_CLASS: ClassVar[Type] = B2Folder

    def __post_init__(self):
        if not self.bucket.replication_configuration:
            raise ValueError(f'Bucket {self.bucket} has no replication configuration')

        if self.rule not in self.bucket.replication_configuration.rules:
            raise ValueError(f'Rule {self.rule} is not a rule from {self.configuration=}')

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

    def iter_pairs(self) -> Iterator[Tuple[Optional[B2Path], Optional[B2Path]]]:
        """
        Iterate over files in source and destination and yield pairs that differ.
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

    def scan_source(self) -> ReplicationReport:
        """
        Scan source bucket only and return replication report.

        This may give limited replication information, since it only
        checks files on the source bucket without checking whether
        they we really replicated to destination.

        May be handy if there is no access to replication destination.
        """
        report = self.replication_report_class()
        for path in self.source_folder.all_files(
            policies_manager=self.scan_policies_manager,
            reporter=self.report,
        ):
            report.add(path)
        return report

    def scan_source_and_destination(self) -> ReplicationReport:
        """
        Scan both source and destination and return replication report.

        This is an in-depth scan, with comparison of source and destination
        files.
        """
        report = self.replication_report_class()
        for pair in self.iter_pairs():
            report.add(*pair)
        return report
