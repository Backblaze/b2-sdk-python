######################################################################
#
# File: b2sdk/_internal/encryption/types.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from enum import Enum, unique


@unique
class EncryptionAlgorithm(Enum):
    """Encryption algorithm."""

    AES256 = 'AES256'

    def get_length(self) -> int:
        if self is EncryptionAlgorithm.AES256:
            return int(256 / 8)

        raise NotImplementedError()


@unique
class EncryptionMode(Enum):
    """Encryption mode."""

    UNKNOWN = None  #: unknown encryption mode (sdk doesn't know or used key has no rights to know)
    NONE = "none"  #: no encryption (plaintext)
    SSE_B2 = 'SSE-B2'  #: server-side encryption with key maintained by B2
    SSE_C = 'SSE-C'  #: server-side encryption with key provided by the client

    #CLIENT = 'CLIENT'  #: client-side encryption

    def can_be_set_as_bucket_default(self):
        return self in BUCKET_DEFAULT_ENCRYPTION_MODES


ENCRYPTION_MODES_WITH_MANDATORY_ALGORITHM = frozenset(
    (EncryptionMode.SSE_B2, EncryptionMode.SSE_C)
)  # yapf: off
ENCRYPTION_MODES_WITH_MANDATORY_KEY = frozenset((EncryptionMode.SSE_C,))  # yapf: off
BUCKET_DEFAULT_ENCRYPTION_MODES = frozenset((EncryptionMode.NONE, EncryptionMode.SSE_B2))
