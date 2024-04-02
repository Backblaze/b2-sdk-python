######################################################################
#
# File: b2sdk/_internal/transfer/inbound/downloader/stats_collector.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
from dataclasses import (
    dataclass,
    field,
)
from time import perf_counter_ns
from typing import (
    Any,
)

logger = logging.getLogger(__name__)


class SingleStatsCollector:
    TO_MS = 1_000_000

    def __init__(self):
        self.latest_entry: int | None = None
        self.sum_of_all_entries: int = 0
        self.started_perf_timer: int | None = None

    def __enter__(self) -> None:
        self.started_perf_timer = perf_counter_ns()

    def __exit__(self, exc_type: type, exc_val: Exception, exc_tb: Any) -> None:
        time_diff = perf_counter_ns() - self.started_perf_timer
        self.latest_entry = time_diff
        self.sum_of_all_entries += time_diff
        self.started_perf_timer = None

    @property
    def sum_ms(self) -> float:
        return self.sum_of_all_entries / self.TO_MS

    @property
    def latest_ms(self) -> float:
        return self.latest_entry / self.TO_MS

    @property
    def has_any_entry(self) -> bool:
        return self.latest_entry is not None


@dataclass
class StatsCollector:
    name: str  #: file name or object url
    detail: str  #: description of the thread, ex. "10000000:20000000" or "writer"
    other_name: str  #: other statistic, typically "seek" or "hash"
    total: SingleStatsCollector = field(default_factory=SingleStatsCollector)
    other: SingleStatsCollector = field(default_factory=SingleStatsCollector)
    write: SingleStatsCollector = field(default_factory=SingleStatsCollector)
    read: SingleStatsCollector = field(default_factory=SingleStatsCollector)

    def report(self):
        if self.read.has_any_entry:
            logger.info('download stats | %s | TTFB: %.3f ms', self, self.read.latest_ms)
            logger.info(
                'download stats | %s | read() without TTFB: %.3f ms', self,
                (self.read.sum_of_all_entries - self.read.latest_entry) / self.read.TO_MS
            )
        if self.other.has_any_entry:
            logger.info(
                'download stats | %s | %s total: %.3f ms', self, self.other_name, self.other.sum_ms
            )
        if self.write.has_any_entry:
            logger.info('download stats | %s | write() total: %.3f ms', self, self.write.sum_ms)
        if self.total.has_any_entry:
            basic_operation_time = self.write.sum_of_all_entries \
                                   + self.other.sum_of_all_entries \
                                   + self.read.sum_of_all_entries
            overhead = self.total.sum_of_all_entries - basic_operation_time
            logger.info(
                'download stats | %s | overhead: %.3f ms', self, overhead / self.total.TO_MS
            )

    def __str__(self):
        return f'{self.name}[{self.detail}]'
