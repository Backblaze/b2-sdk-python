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
        assert self.mode == EncryptionMode.NONE or isinstance(self.algorithm, EncryptionAlgorithm)  # TODO
        if self.mode == EncryptionMode.NONE:
            if self.algorithm or self.key:
                raise ValueError("cannot specify algorithm or key for 'plaintext' encryption mode")
        elif self.mode in ENCRYPTION_MODES_WITH_MANDATORY_KEY and not self.key:
            raise ValueError(
                'must specify key for encryption mode %s and algorithm %s' %
                (self.mode, self.algorithm)
            )

    def __eq__(self, other):
        if other is None:
            raise ValueError('cannot compare a known encryption setting to an unknown one')
        return self.mode == other.mode and self.algorithm == other.algorithm and self.key == other.key

    def as_value_dict(self):
        result = {'mode': self.mode.value}
        if self.algorithm is not None:
            result['algorithm'] = self.algorithm.value
        return result

    def __repr__(self):
        key_repr = '******'
        if self.key is None:
            key_repr = None
        return '<%s(%s, %s, %s)>' % (self.__class__.__name__, self.mode, self.algorithm, key_repr)


class EncryptionSettingFactory:
    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> Optional[EncryptionSetting]:
        """
        Returns EncryptionSetting for the given bucket dict retrieved from the api

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
            kwargs['algorithm'] = EncryptionAlgorithm(algorithm)

        return EncryptionSetting(**kwargs)
