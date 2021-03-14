######################################################################
#
# File: b2sdk/encryption/setting.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
from typing import Optional

from .types import EncryptionAlgorithm, EncryptionKey, EncryptionMode
from .types import ENCRYPTION_MODES_WITH_MANDATORY_KEY

logger = logging.getLogger(__name__)


class EncryptionSetting:
    """
    Hold information about encryption mode, algorithm and key (for bucket default, file version info or even upload)
    """

    def __init__(
        self,
        mode: EncryptionMode,
        algorithm: EncryptionAlgorithm = None,
        key: EncryptionKey = None,
    ):
        self.mode = mode
        self.algorithm = algorithm
        self.key = key
        if self.mode == EncryptionMode.NONE:
            if self.algorithm or self.key:
                raise ValueError("cannot specify algorithm or key for 'plaintext' encryption mode")
        elif self.mode in ENCRYPTION_MODES_WITH_MANDATORY_KEY and not self.key:
            raise ValueError(
                "must specify key for encryption mode %s and algorithm %s" %
                (self.mode, self.algorithm)
            )

    def __eq__(self, other):
        if other is None:
            raise ValueError('cannot compare a known encryption setting to an unknown one')
        return self.mode == other.mode and self.algorithm == other.algorithm and self.key == other.key

    def value_as_dict(self):
        result = {'mode': self.mode.value}
        #if result['mode'] == 'none':
        #    result['mode'] = None
        if self.algorithm is not None:
            result['algorithm'] = self.algorithm.value
        #print('result:', result)
        return result

    def __repr__(self):
        return '<EncryptionSetting(%s, %s)>' % (self.mode, self.algorithm)


class EncryptionSettingFactory:
    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> Optional[EncryptionSetting]:
        """
        Returns EncryptionSetting for the given bucket or None when unknown (unauthorized?).
        Example inputs:

        .. code-block:: python

            ...
            "defaultServerSideEncryption": {
                "isClientAuthorizedToRead" : true,
                "value": {
                  "algorithm" : "AES256",
                  "mode" : "SSE-B2"
                }
            }

        unset:

        .. code-block:: python

             ...
            "defaultServerSideEncryption": {
                "isClientAuthorizedToRead" : true,
                "value": {
                  "mode" : "none"
                }
            }
            ...

        unknown:

        .. code-block:: python

            ...
            "defaultServerSideEncryption": {
                "isClientAuthorizedToRead" : false
            }
            ...

        """
        default_sse = bucket_dict.get(
            'defaultServerSideEncryption',
            {'isClientAuthorizedToRead': False},
        )

        if not default_sse['isClientAuthorizedToRead']:
            return None
        kwargs = {'mode': EncryptionMode(default_sse['value']['mode'])}

        algorithm = default_sse['value'].get('algorithm')
        if algorithm is not None:
            kwargs['algorithm'] = algorithm

        return EncryptionSetting(**kwargs)
