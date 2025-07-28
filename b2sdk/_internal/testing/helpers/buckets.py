######################################################################
#
# File: test/integration/helpers.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import secrets

from b2sdk.v3 import (
    BUCKET_NAME_CHARS_UNIQ,
    BUCKET_NAME_LENGTH_RANGE,
)

GENERAL_BUCKET_NAME_PREFIX = 'sdktst'
BUCKET_NAME_LENGTH = BUCKET_NAME_LENGTH_RANGE[1]
BUCKET_CREATED_AT_MILLIS = 'created_at_millis'

RNG = secrets.SystemRandom()


def _bucket_name_prefix_part(length: int) -> str:
    return ''.join(RNG.choice(BUCKET_NAME_CHARS_UNIQ) for _ in range(length))


def get_bucket_name_prefix(rnd_len: int = 8) -> str:
    return GENERAL_BUCKET_NAME_PREFIX + _bucket_name_prefix_part(rnd_len)


def random_bucket_name(prefix: str = GENERAL_BUCKET_NAME_PREFIX) -> str:
    return prefix + _bucket_name_prefix_part(BUCKET_NAME_LENGTH - len(prefix))