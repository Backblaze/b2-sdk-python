######################################################################
#
# File: b2sdk/_internal/replication/types.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from enum import Enum, unique


@unique
class ReplicationStatus(Enum):
    PENDING = 'PENDING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    REPLICA = 'REPLICA'

    @classmethod
    def from_response_headers(cls, headers: dict) -> ReplicationStatus | None:
        value = headers.get('X-Bz-Replication-Status', None)
        return value and cls[value.upper()]
