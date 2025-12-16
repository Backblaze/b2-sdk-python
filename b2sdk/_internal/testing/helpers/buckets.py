######################################################################
#
# File: b2sdk/_internal/testing/helpers/buckets.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import os
import platform
import random
import secrets
import time
from hashlib import sha256

from b2sdk._internal.http_constants import BUCKET_NAME_CHARS_UNIQ, BUCKET_NAME_LENGTH_RANGE

logger = logging.getLogger(__name__)

GENERAL_BUCKET_NAME_PREFIX = 'sdktst'
BUCKET_NAME_LENGTH = BUCKET_NAME_LENGTH_RANGE[1]
BUCKET_CREATED_AT_MILLIS = 'created_at_millis'
NODE_DESCRIPTION = f'{platform.node()}: {platform.platform()} {platform.python_version()}'


def get_seed() -> str:
    """
    Get seed for random number generator.

    The `WORKFLOW_ID` variable has to be set in the CI to uniquely identify
    the current workflow (including the attempt)
    """
    seed = ''.join(
        (
            os.getenv('WORKFLOW_ID', secrets.token_hex(8)),
            NODE_DESCRIPTION,
            str(time.time_ns()),
            os.getenv('PYTEST_XDIST_WORKER', 'master'),
        )
    )
    return sha256(seed.encode()).hexdigest()[:16]


RNG_SEED = get_seed()
RNG = random.Random(RNG_SEED)
RNG_COUNTER = 0


def random_token(length: int, chars: str = BUCKET_NAME_CHARS_UNIQ) -> str:
    return ''.join(RNG.choice(chars) for _ in range(length))


def bucket_name_part(length: int) -> str:
    assert length >= 1
    global RNG_COUNTER
    RNG_COUNTER += 1
    name_part = random_token(length, BUCKET_NAME_CHARS_UNIQ)
    logger.info('RNG_SEED: %s', RNG_SEED)
    logger.info('RNG_COUNTER: %i, length: %i', RNG_COUNTER, length)
    logger.info('name_part: %s', name_part)
    logger.info('WORKFLOW_ID: %s', os.getenv('WORKFLOW_ID'))
    return name_part


def get_bucket_name_prefix(rnd_len: int = 8, prefix: str = GENERAL_BUCKET_NAME_PREFIX) -> str:
    return prefix + bucket_name_part(rnd_len)
