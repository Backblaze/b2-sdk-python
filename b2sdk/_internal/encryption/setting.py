######################################################################
#
# File: b2sdk/_internal/encryption/setting.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import enum
import logging
import urllib
import urllib.parse

from ..http_constants import SSE_C_KEY_ID_FILE_INFO_KEY_NAME, SSE_C_KEY_ID_HEADER
from ..utils import b64_of_bytes, md5_of_bytes
from .types import (
    ENCRYPTION_MODES_WITH_MANDATORY_ALGORITHM,
    ENCRYPTION_MODES_WITH_MANDATORY_KEY,
    EncryptionAlgorithm,
    EncryptionMode,
)

logger = logging.getLogger(__name__)


class _UnknownKeyId(enum.Enum):
    """The purpose of this enum is to provide a sentinel that can be used with type annotations."""
    unknown_key_id = 0


UNKNOWN_KEY_ID = _UnknownKeyId.unknown_key_id
"""
Value for EncryptionKey.key_id that signifies that the key id may or may not be defined. Useful when establishing
encryption settings based on user input (and not based on B2 cloud data).
"""


class EncryptionKey:
    """
    Hold information about encryption key: the key itself, and its id. The id may be None, if it's not set
    in encrypted file's fileInfo, or UNKNOWN_KEY_ID when that information is missing.
    The secret may be None, if encryption metadata is read from the server.
    """
    SECRET_REPR = '******'

    def __init__(self, secret: bytes | None, key_id: str | None | _UnknownKeyId):
        self.secret = secret
        self.key_id = key_id

    def __eq__(self, other):
        return self.secret == other.secret and self.key_id == other.key_id

    def __repr__(self):
        key_repr = self.SECRET_REPR
        if self.secret is None:
            key_repr = None

        if self.key_id is UNKNOWN_KEY_ID:
            key_id_repr = 'unknown'
        else:
            key_id_repr = repr(self.key_id)
        return f'<{self.__class__.__name__}({key_repr}, {key_id_repr})>'

    def as_dict(self):
        """
        Dump EncryptionKey as dict for serializing a to json for requests.
        """
        if self.secret is not None:
            return {
                'customerKey': self.key_b64(),
                'customerKeyMd5': self.key_md5(),
            }
        return {
            'customerKey': self.SECRET_REPR,
            'customerKeyMd5': self.SECRET_REPR,
        }

    def key_b64(self):
        return b64_of_bytes(self.secret)

    def key_md5(self):
        return b64_of_bytes(md5_of_bytes(self.secret))


class EncryptionSetting:
    """
    Hold information about encryption mode, algorithm and key (for bucket default,
    file version info or even upload)
    """

    def __init__(
        self,
        mode: EncryptionMode,
        algorithm: EncryptionAlgorithm = None,
        key: EncryptionKey = None,
    ):
        """
        :param b2sdk.v2.EncryptionMode mode: encryption mode
        :param b2sdk.v2.EncryptionAlgorithm algorithm: encryption algorithm
        :param b2sdk.v2.EncryptionKey key: encryption key object for SSE-C
        """
        self.mode = mode
        self.algorithm = algorithm
        self.key = key
        if self.mode == EncryptionMode.NONE and (self.algorithm or self.key):
            raise ValueError("cannot specify algorithm or key for 'plaintext' encryption mode")
        if self.mode in ENCRYPTION_MODES_WITH_MANDATORY_ALGORITHM and not self.algorithm:
            raise ValueError(f'must specify algorithm for encryption mode {self.mode}')
        if self.mode in ENCRYPTION_MODES_WITH_MANDATORY_KEY and not self.key:
            raise ValueError(
                f'must specify key for encryption mode {self.mode} and algorithm {self.algorithm}'
            )

    def __eq__(self, other):
        if other is None:
            raise ValueError('cannot compare a known encryption setting to an unknown one')
        return self.mode == other.mode and self.algorithm == other.algorithm and self.key == other.key

    def serialize_to_json_for_request(self):
        if self.key and self.key.secret is None:
            raise ValueError('cannot use an unknown key in requests')
        return self.as_dict()

    def as_dict(self):
        """
        Represent the setting as a dict, for example:

        .. code-block:: python

            {
                'mode': 'SSE-C',
                'algorithm': 'AES256',
                'customerKey': 'U3hWbVlxM3Q2djl5JEImRSlIQE1jUWZUalduWnI0dTc=',
                'customerKeyMd5': 'SWx9GFv5BTT1jdwf48Bx+Q=='
            }

        .. code-block:: python

            {
                'mode': 'SSE-B2',
                'algorithm': 'AES256'
            }

        or

        .. code-block:: python

            {
                'mode': 'none'
            }
        """
        result = {'mode': self.mode.value}
        if self.algorithm is not None:
            result['algorithm'] = self.algorithm.value
        if self.mode == EncryptionMode.SSE_C:
            result.update(self.key.as_dict())
        return result

    def add_to_upload_headers(self, headers):
        if self.mode == EncryptionMode.NONE:
            # as of 2021-03-16, server always fails it
            headers['X-Bz-Server-Side-Encryption'] = self.mode.name
        elif self.mode == EncryptionMode.SSE_B2:
            self._add_sse_b2_headers(headers)
        elif self.mode == EncryptionMode.SSE_C:
            if self.key.key_id is UNKNOWN_KEY_ID:
                raise ValueError('Cannot upload a file with an unknown key id')
            self._add_sse_c_headers(headers)
            if self.key.key_id is not None:
                header = SSE_C_KEY_ID_HEADER
                if headers.get(header) is not None and headers[header] != self.key.key_id:
                    raise ValueError(
                        f'Ambiguous key id set: "{headers[header]}" in headers and "{self.key.key_id}" in {self.__class__.__name__}'
                    )
                headers[header] = urllib.parse.quote(str(self.key.key_id))
        else:
            raise NotImplementedError(f'unsupported encryption setting: {self}')

    def add_to_download_headers(self, headers):
        if self.mode == EncryptionMode.NONE:
            return
        elif self.mode == EncryptionMode.SSE_B2:
            self._add_sse_b2_headers(headers)
        elif self.mode == EncryptionMode.SSE_C:
            self._add_sse_c_headers(headers)
        else:
            raise NotImplementedError(f'unsupported encryption setting: {self}')

    def _add_sse_b2_headers(self, headers):
        headers['X-Bz-Server-Side-Encryption'] = self.algorithm.name

    def _add_sse_c_headers(self, headers):
        if self.key.secret is None:
            raise ValueError('Cannot use an unknown key in http headers')
        headers['X-Bz-Server-Side-Encryption-Customer-Algorithm'] = self.algorithm.name
        headers['X-Bz-Server-Side-Encryption-Customer-Key'] = self.key.key_b64()
        headers['X-Bz-Server-Side-Encryption-Customer-Key-Md5'] = self.key.key_md5()

    def add_key_id_to_file_info(self, file_info: dict | None):
        if self.key is None or self.key.key_id is None:
            return file_info
        if self.key.key_id is UNKNOWN_KEY_ID:
            raise ValueError('Cannot add an unknown key id to file info')
        if file_info is None:
            file_info = {}
        if file_info.get(SSE_C_KEY_ID_FILE_INFO_KEY_NAME) is not None and file_info[
            SSE_C_KEY_ID_FILE_INFO_KEY_NAME] != self.key.key_id:
            raise ValueError(
                'Ambiguous key id set: "{}" in file_info and "{}" in {}'.format(
                    file_info[SSE_C_KEY_ID_FILE_INFO_KEY_NAME], self.key.key_id,
                    self.__class__.__name__
                )
            )
        file_info[SSE_C_KEY_ID_FILE_INFO_KEY_NAME] = self.key.key_id
        return file_info

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.mode}, {self.algorithm}, {self.key})>'

    def is_unknown(self):
        return self.mode == EncryptionMode.NONE


class EncryptionSettingFactory:
    # 2021-03-17: for the bucket the response of the server is:
    # if authorized to read:
    #    "mode": "none"
    #    or
    #    "mode": "SSE-B2"
    # if not authorized to read:
    #    isClientAuthorizedToRead is False and there is no value, so no mode
    #
    # BUT file_version (get_file_info, list_file_versions, upload_file etc)
    # if the file is encrypted, then
    #     "serverSideEncryption": {"algorithm": "AES256", "mode": "SSE-B2"},
    #     or
    #     "serverSideEncryption": {"algorithm": "AES256", "mode": "SSE-C"},
    # if the file is not encrypted, then "serverSideEncryption" is not present at all
    @classmethod
    def from_file_version_dict(cls, file_version_dict: dict) -> EncryptionSetting:
        """
        Returns EncryptionSetting for the given file_version_dict retrieved from the api

        .. code-block:: python

            ...
            "serverSideEncryption": {"algorithm": "AES256", "mode": "SSE-B2"},
            "fileInfo": {"sse_c_key_id": "key-identifier"}
            ...

        """
        sse = file_version_dict.get('serverSideEncryption')
        if sse is None:
            return EncryptionSetting(EncryptionMode.NONE)
        key_id = None
        file_info = file_version_dict.get('fileInfo')
        if file_info is not None and SSE_C_KEY_ID_FILE_INFO_KEY_NAME in file_info:
            key_id = urllib.parse.unquote(file_info[SSE_C_KEY_ID_FILE_INFO_KEY_NAME])

        return cls._from_value_dict(sse, key_id=key_id)

    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> EncryptionSetting | None:
        """
        Returns EncryptionSetting for the given bucket dict retrieved from the api, or None if unauthorized

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
            ...

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
            return EncryptionSetting(EncryptionMode.UNKNOWN)

        assert 'value' in default_sse
        return cls._from_value_dict(default_sse['value'])

    @classmethod
    def _from_value_dict(cls, value_dict, key_id=None):
        kwargs = {}
        if value_dict is None:
            kwargs['mode'] = EncryptionMode.NONE
        else:
            mode = EncryptionMode(value_dict['mode'] or 'none')
            kwargs['mode'] = mode

            algorithm = value_dict.get('algorithm')
            if algorithm is not None:
                kwargs['algorithm'] = EncryptionAlgorithm(algorithm)

            if mode == EncryptionMode.SSE_C:
                kwargs['key'] = EncryptionKey(key_id=key_id, secret=None)

        return EncryptionSetting(**kwargs)

    @classmethod
    def from_response_headers(cls, headers):
        if 'X-Bz-Server-Side-Encryption' in headers:
            mode = EncryptionMode.SSE_B2
            algorithm = EncryptionAlgorithm(headers['X-Bz-Server-Side-Encryption'])
            return EncryptionSetting(mode, algorithm)
        if 'X-Bz-Server-Side-Encryption-Customer-Algorithm' in headers:
            mode = EncryptionMode.SSE_C
            algorithm = EncryptionAlgorithm(
                headers['X-Bz-Server-Side-Encryption-Customer-Algorithm']
            )
            key_id = headers.get(SSE_C_KEY_ID_HEADER)
            key = EncryptionKey(secret=None, key_id=key_id)
            return EncryptionSetting(mode, algorithm, key)
        return EncryptionSetting(EncryptionMode.NONE)


SSE_NONE = EncryptionSetting(mode=EncryptionMode.NONE,)
"""
Commonly used "no encryption" setting
"""

SSE_B2_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_B2,
    algorithm=EncryptionAlgorithm.AES256,
)
"""
Commonly used SSE-B2 setting
"""
