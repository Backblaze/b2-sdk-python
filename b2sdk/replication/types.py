######################################################################
#
# File: b2sdk/replication/types.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from enum import Enum, unique
from typing import Optional


@unique
class ReplicationStatus(Enum):
    PENDING = 'PENDING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    REPLICA = 'REPLICA'

    @classmethod
    def from_response_headers(cls, headers: dict) -> Optional['ReplicationStatus']:
        value = headers.get('X-Bz-Replication-Status', None)
        return value and cls[value.upper()]


@unique
class CompletedReplicationStatus(Enum):
    """
    Substatus for FileVersions where ReplicationStatus was COMPLETED.
    """
    HAS_HIDDEN_MARKER = 'HAS_HIDDEN_MARKER'
    HAS_SSE_C_ENABLED = 'HAS_SSE_C_ENABLED'
    HAS_LARGE_METADATA = 'HAS_LARGE_METADATA'
