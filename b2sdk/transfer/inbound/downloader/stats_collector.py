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
    name: str
    total: Optional[int] = None
    seek: List[int] = field(default_factory=list)
    write: List[int] = field(default_factory=list)
    get: List[int] = field(default_factory=list)
    def report(self):
        logger.info('download stats | %s | TTFB: %.3f ms', self.name, self.get[0] / 1000000)
        logger.info('download stats | %s | get() without TTFB: %.3f ms',self.name,  sum(self.get[1:]) / 1000000)
        logger.info('download stats | %s | seek() total: %.3f ms', self.name, sum(self.seek) / 1000000)
        logger.info('download stats | %s | write() total: %.3f ms', self.name, sum(self.write) / 1000000)
        if self.total is not None:
            overhead = self.total - sum(self.write) - sum(self.seek) - sum(self.get)
            logger.info('download stats | %s | overhead: %.3f ms', self.name, overhead / 1000000)
