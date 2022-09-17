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
from dataclasses import dataclass, field
from typing import List  # 3.7 doesn't understand `list` vs `List`
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class StatsCollector:
    name: str  #: file name or object url
    detail: str  #: description of the thread, ex. "10000000:20000000" or "writer"
    other_name: str  #: other statistic, typically "seek" or "hash"
    total: Optional[int] = None
    other: List[int] = field(default_factory=list)
    write: List[int] = field(default_factory=list)
    read: List[int] = field(default_factory=list)

    def report(self):
        if self.read:
            logger.info('download stats | %s | TTFB: %.3f ms', self, self.read[0] / 1000000)
            logger.info(
                'download stats | %s | read() without TTFB: %.3f ms', self,
                sum(self.read[1:]) / 1000000
            )
        if self.other:
            logger.info(
                'download stats | %s | %s total: %.3f ms', self, self.other_name,
                sum(self.other) / 1000000
            )
        if self.write:
            logger.info(
                'download stats | %s | write() total: %.3f ms', self,
                sum(self.write) / 1000000
            )
        if self.total is not None:
            overhead = self.total - sum(self.write) - sum(self.other) - sum(self.read)
            logger.info('download stats | %s | overhead: %.3f ms', self, overhead / 1000000)

    def __str__(self):
        return f'{self.name}[{self.detail}]'
