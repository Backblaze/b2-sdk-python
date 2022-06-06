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
from dataclasses import dataclass, field
from typing import Dict, Iterator, Optional, Tuple, Union

from ..api import B2Api
from ..bucket import Bucket
from ..encryption.setting import EncryptionMode
from ..file_version import FileVersion
from ..scan.folder import B2Folder
from ..scan.path import B2Path
from ..scan.policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from ..scan.report import Report
from ..scan.scan import zip_folders
from .setting import ReplicationRule
from .types import ReplicationStatus


class FileReplicationStatus:
    pass


@dataclass(frozen=True)
class SourceFileReplicationStatus(FileReplicationStatus):
    replication_status: ReplicationStatus
    has_hide_marker: bool
    has_sse_c_enabled: bool
    has_large_metadata: bool
    # TODO: legal_hold
    # TODO: retention

    @classmethod
    def from_file(cls, file: B2Path) -> 'SourceFileReplicationStatus':
        file_version = file.selected_version
        return cls(
            replication_status=file_version.replication_status,
            has_hide_marker=file.is_visible(),
            has_sse_c_enabled=file_version.server_side_encryption.mode == EncryptionMode.SSE_C,
            has_large_metadata=file_version.has_large_metadata,
        )


@dataclass(frozen=True)
class SourceAndDestinationFileReplicationStatus(FileReplicationStatus):
    replication_status: Tuple[ReplicationStatus, ReplicationStatus]
    # TODO: more features

    @classmethod
    def from_files(cls, source_file: B2Path, destination_file: B2Path) -> 'SourceAndDestinationFileReplicationStatus':
        source_version = source_file.selected_version
        destination_version = destination_file.selected_version

        return cls(
            replication_status=(source_version.replication_status, destination_version.replication_status),
        )


@dataclass
class ReplicationReport:
    counter_by_status: Counter[FileReplicationStatus] = field(default_factory=Counter)
    samples_by_status: Dict[FileReplicationStatus, Union[FileVersion, Tuple[FileVersion, FileVersion]]] = field(default_factory=dict)

    def add(self, source_file: B2Path, destination_file: Optional[B2Path] = None):
        if destination_file:
            status = SourceAndDestinationFileReplicationStatus.from_files(source_file, destination_file)
        else:
            status = SourceFileReplicationStatus.from_file(source_file)
        self.counter_by_status[status] += 1

        if status not in self.samples_by_status:
            if destination_file:
                self.samples_by_status[status] = (source_file.selected_version, destination_file.selected_version)
            else:
                self.samples_by_status[status] = source_file.selected_version


@dataclass
class ReplicationMonitor:
    bucket: Bucket
    rule: ReplicationRule
    destination_api: Optional[B2Api] = None  # if None -> will use `api` of source (bucket)

    report: Report = field(default_factory=Report)
    scan_policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER

    def __post_init__(self):
        if not self.bucket.replication_configuration:
            raise ValueError(f'Bucket {self.bucket} has no replication configuration')

        if self.rule not in self.bucket.replication_configuration.rules:
            raise ValueError(f'Rule {self.rule} is not a rule from {self.configuration=}')

    @property
    def source_api(self) -> B2Api:
        return self.bucket.api

    # TODO: remove this
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

    def iter_diff(self) -> Iterator[Tuple[Optional[B2Path], Optional[B2Path]]]:
        """
        Iterate over files in source and destination and yield pairs that differ.
        Required for replication inspection in-depth.
        """
        yield from zip_folders(
            self.source_folder,
            self.destination_folder,
            report=self.report,
            policies_manager=self.scan_policies_manager,
        )

    def scan_source(self) -> ReplicationReport:
        report = ReplicationReport()
        for path in self.source_folder.all_files(policies_manager=self.scan_policies_manager):
            report.add(path)
        return report

    def scan_source_and_destination(self) -> ReplicationReport:
        report = ReplicationReport()
        for pair in self.iter_diff():
            report.add(*pair)
        return report
