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
from typing import Optional, NewType

from ..raw_api import EncryptionMode, EncryptionAlgorithm
from ..utils import B2TraceMeta, limit_trace_arguments

logger = logging.getLogger(__name__)

EncryptionKey = NewType('EncryptionKey', bytes)


class EncryptionSetting(metaclass=B2TraceMeta):
    """
    Hold information about encryption mode, algorithm and key (for bucket default, file version info or even upload)
    """

    @limit_trace_arguments(skip=('key',))
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
        else:
            if not self.algorithm:
                raise ValueError("must specify algorithm every non-plaintext encryption mode")
            if self.mode in [EncryptionMode.SSE_C] and not self.key:  # TODO move the list somewhere
                raise ValueError(
                    "must specify key for encryption mode %s and algorithm %s" %
                    (self.mode, self.algorithm)
                )

    def __eq__(self, other):
        if other is None:
            raise ValueError('cannot compare a known encryption setting to an unknown one')
        return self.mode == other.mode and self.algorithm == other.algorithm and self.key == other.key

    def __repr__(self):
        return '<EncryptionSetting(%s, %s)>' % (self.mode, self.algorithm)


class EncryptionSettingFactory:
    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> Optional[EncryptionSetting]:
        """
        Returns EncryptionSetting for the given bucket or None when unknown (unauthorized?).
        Example inputs, set:

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

        print('isClientAuthorizedToRead', default_sse)
        if not default_sse['isClientAuthorizedToRead']:
            return None
        elif default_sse['value']['mode'] == 'none':
            return EncryptionSetting(mode=EncryptionMode('none'))
        else:
            print('default_sse', default_sse['value'])
            return EncryptionSetting(
                mode=EncryptionMode(default_sse['value']['mode']),
                algorithm=EncryptionAlgorithm(default_sse['value']['algorithm']),
            )
