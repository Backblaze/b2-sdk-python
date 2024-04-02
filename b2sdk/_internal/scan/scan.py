######################################################################
#
# File: b2sdk/_internal/scan/scan.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import ABCMeta, abstractclassmethod, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from typing import ClassVar

from ..file_version import FileVersion
from .folder import AbstractFolder
from .path import AbstractPath
from .policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from .report import ProgressReport


def zip_folders(
    folder_a: AbstractFolder,
    folder_b: AbstractFolder,
    reporter: ProgressReport,
    policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER,
) -> tuple[AbstractPath | None, AbstractPath | None]:
    """
    Iterate over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.

    :param b2sdk._internal.scan.folder.AbstractFolder folder_a: first folder object.
    :param b2sdk._internal.scan.folder.AbstractFolder folder_b: second folder object.
    :param reporter: reporter object
    :param policies_manager: policies manager object
    :return: yields two element tuples
    """

    iter_a = folder_a.all_files(reporter, policies_manager)
    iter_b = folder_b.all_files(reporter)

    current_a = next(iter_a, None)
    current_b = next(iter_b, None)

    while current_a is not None or current_b is not None:
        if current_a is None:
            yield (None, current_b)
            current_b = next(iter_b, None)
        elif current_b is None:
            yield (current_a, None)
            current_a = next(iter_a, None)
        elif current_a.relative_path < current_b.relative_path:
            yield (current_a, None)
            current_a = next(iter_a, None)
        elif current_b.relative_path < current_a.relative_path:
            yield (None, current_b)
            current_b = next(iter_b, None)
        else:
            assert current_a.relative_path == current_b.relative_path
            yield (current_a, current_b)
            current_a = next(iter_a, None)
            current_b = next(iter_b, None)

    reporter.close()


@dataclass(frozen=True)
class AbstractScanResult(metaclass=ABCMeta):
    """
    Some attributes of files which are meaningful for monitoring and troubleshooting.
    """

    @abstractclassmethod
    def from_files(cls, *files: AbstractPath | None) -> AbstractScanResult:
        pass


@dataclass
class AbstractScanReport(metaclass=ABCMeta):
    """
    Aggregation of valuable information about files after scanning.
    """
    SCAN_RESULT_CLASS: ClassVar[type] = AbstractScanResult

    @abstractmethod
    def add(self, *files: AbstractPath | None) -> None:
        pass


@dataclass
class CountAndSampleScanReport(AbstractScanReport):
    """
    Scan report which groups and counts files by their `AbstractScanResult` and
    also stores first and last seen examples of such files.
    """
    counter_by_status: Counter = field(default_factory=Counter)
    samples_by_status_first: dict[AbstractScanResult, tuple[FileVersion, ...]] = field(
        default_factory=dict
    )
    samples_by_status_last: dict[AbstractScanResult, tuple[FileVersion, ...]] = field(
        default_factory=dict
    )

    def add(self, *files: AbstractPath | None) -> None:
        status = self.SCAN_RESULT_CLASS.from_files(*files)
        self.counter_by_status[status] += 1

        sample = tuple(file and file.selected_version for file in files)
        self.samples_by_status_first.setdefault(status, sample)
        self.samples_by_status_last[status] = sample
