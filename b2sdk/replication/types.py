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


@unique
class ReplicationStatus(Enum):
    PENDING = 'PENDING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    REPLICA = 'REPLICA'
