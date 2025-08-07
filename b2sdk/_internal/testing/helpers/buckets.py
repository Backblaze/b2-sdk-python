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

import secrets

from b2sdk._internal.http_constants import BUCKET_NAME_CHARS_UNIQ, BUCKET_NAME_LENGTH_RANGE

GENERAL_BUCKET_NAME_PREFIX = 'sdktst'
BUCKET_NAME_LENGTH = BUCKET_NAME_LENGTH_RANGE[1]
BUCKET_CREATED_AT_MILLIS = 'created_at_millis'

RNG = secrets.SystemRandom()


def random_token(length: int, chars: str = BUCKET_NAME_CHARS_UNIQ) -> str:
    return ''.join(RNG.choice(chars) for _ in range(length))


def get_bucket_name_prefix(rnd_len: int = 8) -> str:
    return GENERAL_BUCKET_NAME_PREFIX + random_token(rnd_len)
