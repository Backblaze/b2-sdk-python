######################################################################
#
# File: b2sdk/transfer/inbound/downloader/stats_collector.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
from dataclasses import (
    dataclass,
    field,
)
from time import perf_counter_ns
from typing import (
    Any,
    List,
    Optional,
    Type,  # 3.7 doesn't understand `list` vs `List`
)

logger = logging.getLogger(__name__)


class SingleStatsCollector:
    def __init__(self):
        self.storage: List[int] = []
        self.started_perf_timer: Optional[int] = None

    def __enter__(self) -> None:
        self.started_perf_timer = perf_counter_ns()

    def __exit__(self, exc_type: Type, exc_val: Exception, exc_tb: Any) -> None:
        time_diff = perf_counter_ns() - self.started_perf_timer
        self.storage.append(time_diff)
        self.started_perf_timer = None

    def __getitem__(self, item_or_slice):
        return self.storage[item_or_slice]

    @property
    def has_any_entry(self) -> bool:
        return len(self.storage) > 0


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
            logger.info('download stats | %s | TTFB: %.3f ms', self, self.read[0] / 1000000)
            logger.info(
                'download stats | %s | read() without TTFB: %.3f ms', self,
                sum(self.read[1:]) / 1000000
            )
        if self.other.has_any_entry:
            logger.info(
                'download stats | %s | %s total: %.3f ms', self, self.other_name,
                sum(self.other) / 1000000
            )
        if self.write.has_any_entry:
            logger.info(
                'download stats | %s | write() total: %.3f ms', self,
                sum(self.write) / 1000000
            )
        if self.total.has_any_entry:
            overhead = sum(self.total) - sum(self.write) - sum(self.other) - sum(self.read)
            logger.info('download stats | %s | overhead: %.3f ms', self, overhead / 1000000)

    def __str__(self):
        return f'{self.name}[{self.detail}]'
