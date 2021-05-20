######################################################################
#
# File: b2sdk/raw_simulator.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional
import collections
import io
import logging
import random
import re
import time
import threading
from contextlib import contextmanager

from .b2http import ResponseContextManager
from .encryption.setting import EncryptionMode, EncryptionSetting
from .exception import (
    BadJson,
    BadUploadUrl,
    ChecksumMismatch,
    Conflict,
    DuplicateBucketName,
    FileNotPresent,
    FileSha1Mismatch,
    InvalidAuthToken,
    InvalidMetadataDirective,
    MissingPart,
    NonExistentBucket,
    PartSha1Mismatch,
    Unauthorized,
    UnsatisfiableRange,
    SSECKeyError,
)
from .file_lock import BucketRetentionSetting, FileRetentionSetting, NO_RETENTION_BUCKET_SETTING, UNKNOWN_FILE_LOCK_CONFIGURATION, LegalHold
from .raw_api import AbstractRawApi, HEX_DIGITS_AT_END, MetadataDirectiveMode, ALL_CAPABILITIES
from .utils import (
    b2_url_decode,
    b2_url_encode,
    ConcurrentUsedAuthTokenGuard,
    hex_sha1_of_bytes,
)
from .stream.hashing import StreamWithHash

logger = logging.getLogger(__name__)


def get_bytes_range(data_bytes, bytes_range):
    """ Slice bytes array using bytes range """
    if bytes_range is None:
        return data_bytes
    if bytes_range[0] > bytes_range[1]:
        raise UnsatisfiableRange()
    if bytes_range[0] < 0:
        raise UnsatisfiableRange()
    if bytes_range[1] > len(data_bytes):
        raise UnsatisfiableRange()
    return data_bytes[bytes_range[0]:bytes_range[1]]


class KeySimulator(object):
    """
    Hold information about one application key, which can be either
    a master application key, or one created with create_key().
    """

    def __init__(
        self, account_id, name, application_key_id, key, capabilities, expiration_timestamp_or_none,
        bucket_id_or_none, bucket_name_or_none, name_prefix_or_none
    ):
        self.name = name
        self.account_id = account_id
        self.application_key_id = application_key_id
        self.key = key
        self.capabilities = capabilities
        self.expiration_timestamp_or_none = expiration_timestamp_or_none
        self.bucket_id_or_none = bucket_id_or_none
        self.name_prefix_or_none = name_prefix_or_none

    def as_key(self):
        return dict(
            accountId=self.account_id,
            bucketId=self.bucket_id_or_none,
            applicationKey=self.key,
            applicationKeyId=self.application_key_id,
            capabilities=self.capabilities,
            expirationTimestamp=self.expiration_timestamp_or_none,
            keyName=self.name,
            namePrefix=self.name_prefix_or_none,
        )

    def as_created_key(self):
        """
        Return the dict returned by b2_create_key.

        This is just like the one for b2_list_keys, but also includes the secret key.
        """
        result = self.as_key()
        result['applicationKey'] = self.key
        return result

    def get_allowed(self):
        """
        Return the 'allowed' structure to include in the response from b2_authorize_account.
        """
        return dict(
            bucketId=self.bucket_id_or_none,
            capabilities=self.capabilities,
            namePrefix=self.name_prefix_or_none,
        )


class PartSimulator(object):
    def __init__(self, file_id, part_number, content_length, content_sha1, part_data):
        self.file_id = file_id
        self.part_number = part_number
        self.content_length = content_length
        self.content_sha1 = content_sha1
        self.part_data = part_data

    def as_list_parts_dict(self):
        return dict(
            fileId=self.file_id,
            partNumber=self.part_number,
            contentLength=self.content_length,
            contentSha1=self.content_sha1
        )  # yapf: disable


class FileSimulator(object):
    """
    One of three: an unfinished large file, a finished file, or a deletion marker.
    """

    CHECK_ENCRYPTION = True

    def __init__(
        self,
        account_id,
        bucket,
        file_id,
        action,
        name,
        content_type,
        content_sha1,
        file_info,
        data_bytes,
        upload_timestamp,
        range_=None,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: LegalHold = LegalHold.UNSET,
    ):
        if action == 'hide':
            assert server_side_encryption is None
        else:
            assert server_side_encryption is not None
        self.account_id = account_id
        self.bucket = bucket
        self.file_id = file_id
        self.action = action
        self.name = name
        if data_bytes is not None:
            self.content_length = len(data_bytes)
        self.content_type = content_type
        self.content_sha1 = content_sha1
        if content_sha1 and content_sha1 != 'none' and len(content_sha1) != 40:
            raise ValueError(content_sha1)
        self.file_info = file_info
        self.data_bytes = data_bytes
        self.upload_timestamp = upload_timestamp
        self.range_ = range_
        self.server_side_encryption = server_side_encryption
        self.file_retention = file_retention
        self.legal_hold = legal_hold if legal_hold is not None else LegalHold.UNSET

        if action == 'start':
            self.parts = []

    @classmethod
    @contextmanager
    def dont_check_encryption(cls):
        cls.CHECK_ENCRYPTION = False
        yield
        cls.CHECK_ENCRYPTION = True

    def sort_key(self):
        """
        Return a key that can be used to sort the files in a
        bucket in the order that b2_list_file_versions returns them.
        """
        return (self.name, self.file_id)

    def as_download_headers(self, account_auth_token_or_none, range_=None):
        if self.data_bytes is None:
            content_length = 0
        elif range_ is not None:
            if range_[1] >= len(self.data_bytes):  # requested too much
                content_length = len(self.data_bytes)
            else:
                content_length = range_[1] - range_[0] + 1
        else:
            content_length = len(self.data_bytes)
        headers = {
            'content-length': content_length,
            'content-type': self.content_type,
            'x-bz-content-sha1': self.content_sha1,
            'x-bz-upload-timestamp': self.upload_timestamp,
            'x-bz-file-id': self.file_id,
            'x-bz-file-name': self.name,
        }

        if account_auth_token_or_none is not None and self.bucket.is_file_lock_enabled:
            not_permitted = []

            if not self.is_allowed_to_read_file_retention(account_auth_token_or_none):
                not_permitted.append('X-Bz-File-Retention-Mode')
                not_permitted.append('X-Bz-File-Retain-Until-Timestamp')
            else:
                if self.file_retention is not None:
                    self.file_retention.add_to_to_upload_headers(headers)

            if not self.is_allowed_to_read_file_legal_hold(account_auth_token_or_none):
                not_permitted.append('X-Bz-File-Legal-Hold')
            else:
                headers['X-Bz-File-Legal-Hold'] = self.legal_hold and 'on' or 'off'

            if not_permitted:
                headers['X-Bz-Client-Unauthorized-To-Read'] = ','.join(not_permitted)

        if range_ is not None:
            headers['Content-Range'] = 'bytes %d-%d/%d' % (
                range_[0], range_[0] + content_length, len(self.data_bytes)
            )  # yapf: disable
        for key, value in self.file_info.items():
            headers['x-bz-info-' + key] = value
        return headers

    def as_upload_result(self, account_auth_token):
        result = dict(
            fileId=self.file_id,
            fileName=self.name,
            accountId=self.account_id,
            bucketId=self.bucket.bucket_id,
            contentLength=len(self.data_bytes) if self.data_bytes is not None else 0,
            contentType=self.content_type,
            contentSha1=self.content_sha1,
            fileInfo=self.file_info,
            action=self.action,
            uploadTimestamp=self.upload_timestamp,
        )  # yapf: disable
        if self.server_side_encryption is not None:
            result['serverSideEncryption'
                  ] = self.server_side_encryption.serialize_to_json_for_request()
        result['fileRetention'] = self._file_retention_dict(account_auth_token)
        result['legalHold'] = self._legal_hold_dict(account_auth_token)
        return result

    def as_list_files_dict(self, account_auth_token):
        result = dict(
            fileId=self.file_id,
            fileName=self.name,
            contentLength=len(self.data_bytes) if self.data_bytes is not None else 0,
            contentType=self.content_type,
            contentSha1=self.content_sha1,
            fileInfo=self.file_info,
            action=self.action,
            uploadTimestamp=self.upload_timestamp,
        )  # yapf: disable
        if self.server_side_encryption is not None:
            result['serverSideEncryption'
                  ] = self.server_side_encryption.serialize_to_json_for_request()
        result['fileRetention'] = self._file_retention_dict(account_auth_token)
        result['legalHold'] = self._legal_hold_dict(account_auth_token)
        return result

    def is_allowed_to_read_file_retention(self, account_auth_token):
        return self.bucket._check_capability(account_auth_token, 'readFileRetentions')

    def is_allowed_to_read_file_legal_hold(self, account_auth_token):
        return self.bucket._check_capability(account_auth_token, 'readFileLegalHolds')

    def as_start_large_file_result(self, account_auth_token):
        result = dict(
            fileId=self.file_id,
            fileName=self.name,
            accountId=self.account_id,
            bucketId=self.bucket.bucket_id,
            contentType=self.content_type,
            fileInfo=self.file_info,
            uploadTimestamp=self.upload_timestamp,
        )  # yapf: disable
        if self.server_side_encryption is not None:
            result['serverSideEncryption'
                  ] = self.server_side_encryption.serialize_to_json_for_request()
        result['fileRetention'] = self._file_retention_dict(account_auth_token)
        result['legalHold'] = self._legal_hold_dict(account_auth_token)
        return result

    def _file_retention_dict(self, account_auth_token):
        if not self.is_allowed_to_read_file_retention(account_auth_token):
            return UNKNOWN_FILE_LOCK_CONFIGURATION

        file_lock_configuration = {'isClientAuthorizedToRead': True}
        if self.file_retention is None:
            file_lock_configuration['value'] = {'mode': None}
        else:
            file_lock_configuration['value'] = {'mode': self.file_retention.mode.value}
            if self.file_retention.retain_until is not None:
                file_lock_configuration['value']['retainUntilTimestamp'
                                                ] = self.file_retention.retain_until
        return file_lock_configuration

    def _legal_hold_dict(self, account_auth_token):
        if not self.is_allowed_to_read_file_legal_hold(account_auth_token):
            return {
                'isClientAuthorizedToRead': False,
                'value': None,
            }
        return {
            'isClientAuthorizedToRead': True,
            'value': self.legal_hold.value,
        }

    def add_part(self, part_number, part):
        while len(self.parts) < part_number + 1:
            self.parts.append(None)
        self.parts[part_number] = part

    def finish(self, part_sha1_array):
        last_part_number = max(part.part_number for part in self.parts if part is not None)
        for part_number in range(1, last_part_number + 1):
            if self.parts[part_number] is None:
                raise MissingPart(part_number)
        my_part_sha1_array = [
            self.parts[part_number].content_sha1 for part_number in range(1, last_part_number + 1)
        ]
        if part_sha1_array != my_part_sha1_array:
            raise ChecksumMismatch(
                'sha1', expected=str(part_sha1_array), actual=str(my_part_sha1_array)
            )
        self.data_bytes = b''.join(
            self.parts[part_number].part_data for part_number in range(1, last_part_number + 1)
        )
        self.content_length = len(self.data_bytes)
        self.action = 'upload'

    def is_visible(self):
        """
        Does this file show up in b2_list_file_names?
        """
        return self.action == 'upload'

    def list_parts(self, start_part_number, max_part_count):
        start_part_number = start_part_number or 1
        max_part_count = max_part_count or 100
        parts = [
            part.as_list_parts_dict()
            for part in self.parts if part is not None and start_part_number <= part.part_number
        ]
        if len(parts) <= max_part_count:
            next_part_number = None
        else:
            next_part_number = parts[max_part_count]['partNumber']
            parts = parts[:max_part_count]
        return dict(parts=parts, nextPartNumber=next_part_number)

    def check_encryption(self, request_encryption: Optional[EncryptionSetting]):
        if not self.CHECK_ENCRYPTION:
            return
        file_mode, file_secret = self._get_encryption_mode_and_secret(self.server_side_encryption)
        request_mode, request_secret = self._get_encryption_mode_and_secret(request_encryption)

        if file_mode in (None, EncryptionMode.NONE):
            assert request_mode in (None, EncryptionMode.NONE)
        elif file_mode == EncryptionMode.SSE_B2:
            assert request_mode in (None, EncryptionMode.NONE, EncryptionMode.SSE_B2)
        elif file_mode == EncryptionMode.SSE_C:
            if request_mode != EncryptionMode.SSE_C or file_secret != request_secret:
                raise SSECKeyError()
        else:
            raise ValueError('Unsupported EncryptionMode: %s' % (file_mode))

    def _get_encryption_mode_and_secret(self, encryption: Optional[EncryptionSetting]):
        if encryption is None:
            return None, None
        mode = encryption.mode
        if encryption.key is None:
            secret = None
        else:
            secret = encryption.key.secret
        return mode, secret


FakeRequest = collections.namedtuple('FakeRequest', 'url headers')


class FakeResponse(object):
    def __init__(self, account_auth_token_or_none, file_sim, url, range_=None):
        self.data_bytes = file_sim.data_bytes
        self.headers = file_sim.as_download_headers(account_auth_token_or_none, range_)
        self.url = url
        self.range_ = range_
        if range_ is not None:
            self.data_bytes = self.data_bytes[range_[0]:range_[1] + 1]

    def iter_content(self, chunk_size=1):
        start = 0
        rnd = random.Random(self.url)
        while start <= len(self.data_bytes):
            time.sleep(rnd.random() * 0.01)
            yield self.data_bytes[start:start + chunk_size]
            start += chunk_size

    @property
    def request(self):
        headers = {}
        if self.range_ is not None:
            headers['Range'] = '%s-%s' % self.range_
        return FakeRequest(self.url, headers)

    def close(self):
        pass


class BucketSimulator(object):

    # File IDs start at 9999 and count down, so they sort in the order
    # returned by list_file_versions. The IDs are strings.
    FIRST_FILE_NUMBER = 9999

    FIRST_FILE_ID = str(FIRST_FILE_NUMBER)

    FILE_SIMULATOR_CLASS = FileSimulator
    RESPONSE_CLASS = FakeResponse

    def __init__(
        self,
        api,
        account_id,
        bucket_id,
        bucket_name,
        bucket_type,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        options_set=None,
        default_server_side_encryption=None,
        is_file_lock_enabled: Optional[bool] = None,
    ):
        assert bucket_type in ['allPrivate', 'allPublic']
        self.api = api
        self.account_id = account_id
        self.bucket_name = bucket_name
        self.bucket_id = bucket_id
        self.bucket_type = bucket_type
        self.bucket_info = bucket_info or {}
        self.cors_rules = cors_rules or []
        self.lifecycle_rules = lifecycle_rules or []
        self.options_set = options_set or set()
        self.revision = 1
        self.upload_url_counter = iter(range(200))
        # File IDs count down, so that the most recent will come first when they are sorted.
        self.file_id_counter = iter(range(self.FIRST_FILE_NUMBER, 0, -1))
        self.upload_timestamp_counter = iter(range(5000, 9999))
        self.file_id_to_file = dict()
        # It would be nice to use an OrderedDict for this, but 2.6 doesn't have it.
        self.file_name_and_id_to_file = dict()
        if default_server_side_encryption is None:
            default_server_side_encryption = EncryptionSetting(mode=EncryptionMode.NONE)
        self.default_server_side_encryption = default_server_side_encryption
        self.is_file_lock_enabled = is_file_lock_enabled
        self.default_retention = NO_RETENTION_BUCKET_SETTING

    def is_allowed_to_read_bucket_encryption_setting(self, account_auth_token):
        return self._check_capability(account_auth_token, 'readBucketEncryption')

    def is_allowed_to_read_bucket_retention(self, account_auth_token):
        return self._check_capability(account_auth_token, 'readBucketRetentions')

    def _check_capability(self, account_auth_token, capability):
        try:
            key = self.api.auth_token_to_key[account_auth_token]
        except KeyError:
            # looks like it's an upload token
            # fortunately BucketSimulator makes it easy to retrieve the true account_auth_token
            # from an upload url
            real_auth_token = account_auth_token.split('/')[-1]
            key = self.api.auth_token_to_key[real_auth_token]
        capabilities = key.get_allowed()['capabilities']
        return capability in capabilities

    def bucket_dict(self, account_auth_token):
        default_sse = {'isClientAuthorizedToRead': False}
        if self.is_allowed_to_read_bucket_encryption_setting(account_auth_token):
            default_sse['isClientAuthorizedToRead'] = True
            default_sse['value'] = {'mode': self.default_server_side_encryption.mode.value}
            if self.default_server_side_encryption.algorithm is not None:
                default_sse['value']['algorithm'
                                    ] = self.default_server_side_encryption.algorithm.value
        else:
            default_sse['value'] = {'mode': EncryptionMode.UNKNOWN}

        if self.is_allowed_to_read_bucket_retention(account_auth_token):
            file_lock_configuration = {
                'isClientAuthorizedToRead': True,
                'value': {
                    'defaultRetention': {
                        'mode': self.default_retention.mode.value,
                        'period': self.default_retention.period,
                    },
                    'isFileLockEnabled': self.is_file_lock_enabled,
                },
            }  # yapf: disable
        else:
            file_lock_configuration = {'isClientAuthorizedToRead': False, 'value': None}
        return dict(
            accountId=self.account_id,
            bucketName=self.bucket_name,
            bucketId=self.bucket_id,
            bucketType=self.bucket_type,
            bucketInfo=self.bucket_info,
            corsRules=self.cors_rules,
            lifecycleRules=self.lifecycle_rules,
            options=self.options_set,
            revision=self.revision,
            defaultServerSideEncryption=default_sse,
            fileLockConfiguration=file_lock_configuration,
        )

    def cancel_large_file(self, file_id):
        file_sim = self.file_id_to_file[file_id]
        key = (file_sim.name, file_id)
        del self.file_name_and_id_to_file[key]
        del self.file_id_to_file[file_id]
        return dict(
            accountId=self.account_id,
            bucketId=self.bucket_id,
            fileId=file_id,
            fileName=file_sim.name
        )  # yapf: disable

    def delete_file_version(self, file_id, file_name):
        key = (file_name, file_id)
        file_sim = self.file_name_and_id_to_file[key]
        del self.file_name_and_id_to_file[key]
        del self.file_id_to_file[file_id]
        return dict(fileId=file_id, fileName=file_name, uploadTimestamp=file_sim.upload_timestamp)

    def download_file_by_id(
        self,
        account_auth_token_or_none,
        file_id,
        url,
        range_=None,
        encryption: Optional[EncryptionSetting] = None,
    ):
        file_sim = self.file_id_to_file[file_id]
        file_sim.check_encryption(encryption)
        return self._download_file_sim(account_auth_token_or_none, file_sim, url, range_=range_)

    def download_file_by_name(
        self,
        account_auth_token_or_none,
        file_name,
        url,
        range_=None,
        encryption: Optional[EncryptionSetting] = None,
    ):
        files = self.list_file_names(self.api.current_token, file_name,
                                     1)['files']  # token is not important here
        if len(files) == 0:
            raise FileNotPresent(file_id_or_name=file_name)
        file_dict = files[0]
        if file_dict['fileName'] != file_name or file_dict['action'] != 'upload':
            raise FileNotPresent(file_id_or_name=file_name)
        file_sim = self.file_name_and_id_to_file[(file_name, file_dict['fileId'])]
        file_sim.check_encryption(encryption)
        return self._download_file_sim(account_auth_token_or_none, file_sim, url, range_=range_)

    def _download_file_sim(self, account_auth_token_or_none, file_sim, url, range_=None):
        return ResponseContextManager(
            self.RESPONSE_CLASS(
                account_auth_token_or_none,
                file_sim,
                url,
                range_,
            )
        )

    def finish_large_file(self, account_auth_token, file_id, part_sha1_array):
        file_sim = self.file_id_to_file[file_id]
        file_sim.finish(part_sha1_array)
        return file_sim.as_upload_result(account_auth_token)

    def get_file_info_by_id(self, account_auth_token, file_id):
        return self.file_id_to_file[file_id].as_upload_result(account_auth_token)

    def get_file_info_by_name(self, account_auth_token, file_name):
        for ((name, id), file) in self.file_name_and_id_to_file.items():
            if file_name == name:
                return file.as_download_headers(account_auth_token_or_none=account_auth_token)
        raise FileNotPresent(file_id_or_name=file_name, bucket_name=self.bucket_name)

    def get_upload_url(self, account_auth_token):
        upload_id = next(self.upload_url_counter)
        upload_url = 'https://upload.example.com/%s/%d/%s' % (
            self.bucket_id, upload_id, account_auth_token
        )
        return dict(bucketId=self.bucket_id, uploadUrl=upload_url, authorizationToken=upload_url)

    def get_upload_part_url(self, account_auth_token, file_id):
        upload_url = 'https://upload.example.com/part/%s/%d/%s' % (
            file_id, random.randint(1, 10**9), account_auth_token
        )
        return dict(bucketId=self.bucket_id, uploadUrl=upload_url, authorizationToken=upload_url)

    def hide_file(self, account_auth_token, file_name):
        file_id = self._next_file_id()
        file_sim = self.FILE_SIMULATOR_CLASS(
            self.account_id, self, file_id, 'hide', file_name, None, "none", {}, b'',
            next(self.upload_timestamp_counter)
        )
        self.file_id_to_file[file_id] = file_sim
        self.file_name_and_id_to_file[file_sim.sort_key()] = file_sim
        return file_sim.as_list_files_dict(account_auth_token)

    def update_file_retention(
        self,
        account_auth_token,
        file_id,
        file_name,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ):
        file_sim = self.file_id_to_file[file_id]
        assert self.is_file_lock_enabled
        assert file_sim.name == file_name
        # TODO: check bypass etc
        file_sim.file_retention = file_retention
        return {
            'fileId': file_id,
            'fileName': file_name,
            'fileRetention': file_sim.file_retention.serialize_to_json_for_request(),
        }

    def update_file_legal_hold(
        self,
        account_auth_token,
        file_id,
        file_name,
        legal_hold: LegalHold,
    ):
        file_sim = self.file_id_to_file[file_id]
        assert self.is_file_lock_enabled
        assert file_sim.name == file_name
        file_sim.legal_hold = legal_hold
        return {
            'fileId': file_id,
            'fileName': file_name,
            'legalHold': legal_hold.to_server(),
        }

    def copy_file(
        self,
        file_id,
        new_file_name,
        bytes_range=None,
        metadata_directive=None,
        content_type=None,
        file_info=None,
        destination_bucket_id=None,
        destination_server_side_encryption: Optional[EncryptionSetting] = None,
        source_server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        if metadata_directive is not None:
            assert metadata_directive in tuple(MetadataDirectiveMode)
            if metadata_directive is MetadataDirectiveMode.COPY and (
                content_type is not None or file_info is not None
            ):
                raise InvalidMetadataDirective(
                    'content_type and file_info should be None when metadata_directive is COPY'
                )
            elif metadata_directive is MetadataDirectiveMode.REPLACE and content_type is None:
                raise InvalidMetadataDirective(
                    'content_type cannot be None when metadata_directive is REPLACE'
                )

        file_sim = self.file_id_to_file[file_id]
        file_sim.check_encryption(source_server_side_encryption)
        new_file_id = self._next_file_id()

        data_bytes = get_bytes_range(file_sim.data_bytes, bytes_range)

        destination_bucket = self.api.bucket_id_to_bucket.get(destination_bucket_id, self)
        sse = destination_server_side_encryption or self.default_server_side_encryption
        copy_file_sim = self.FILE_SIMULATOR_CLASS(
            self.account_id,
            destination_bucket,
            new_file_id,
            'copy',
            new_file_name,
            file_sim.content_type,
            file_sim.content_sha1,
            file_sim.file_info,
            data_bytes,
            next(self.upload_timestamp_counter),
            server_side_encryption=sse,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

        if metadata_directive is MetadataDirectiveMode.REPLACE:
            copy_file_sim.content_type = content_type
            copy_file_sim.file_info = file_info or file_sim.file_info

        return copy_file_sim

    def list_file_names(
        self,
        account_auth_token,
        start_file_name=None,
        max_file_count=None,
        prefix=None,
    ):
        assert prefix is None or start_file_name is None or start_file_name.startswith(prefix
                                                                                      ), locals()
        start_file_name = start_file_name or ''
        max_file_count = max_file_count or 100
        result_files = []
        next_file_name = None
        prev_file_name = None
        for key in sorted(self.file_name_and_id_to_file):
            (file_name, file_id) = key
            assert file_id
            if start_file_name <= file_name and file_name != prev_file_name:
                if prefix is not None and not file_name.startswith(prefix):
                    break
                prev_file_name = file_name
                file_sim = self.file_name_and_id_to_file[key]
                if file_sim.is_visible():
                    result_files.append(file_sim.as_list_files_dict(account_auth_token))
                    if len(result_files) == max_file_count:
                        next_file_name = file_sim.name + ' '
                        break
        return dict(files=result_files, nextFileName=next_file_name)

    def list_file_versions(
        self,
        account_auth_token,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None,
        prefix=None,
    ):
        assert prefix is None or start_file_name is None or start_file_name.startswith(prefix
                                                                                      ), locals()
        start_file_name = start_file_name or ''
        start_file_id = start_file_id or ''
        max_file_count = max_file_count or 100
        result_files = []
        next_file_name = None
        next_file_id = None
        for key in sorted(self.file_name_and_id_to_file):
            (file_name, file_id) = key
            if (start_file_name < file_name) or (
                start_file_name == file_name and
                (start_file_id == '' or int(start_file_id) <= int(file_id))
            ):
                file_sim = self.file_name_and_id_to_file[key]
                if prefix is not None and not file_name.startswith(prefix):
                    break
                result_files.append(file_sim.as_list_files_dict(account_auth_token))
                if len(result_files) == max_file_count:
                    next_file_name = file_sim.name
                    next_file_id = str(int(file_id) + 1)
                    break
        return dict(files=result_files, nextFileName=next_file_name, nextFileId=next_file_id)

    def list_parts(self, file_id, start_part_number, max_part_count):
        file_sim = self.file_id_to_file[file_id]
        return file_sim.list_parts(start_part_number, max_part_count)

    def list_unfinished_large_files(
        self, account_auth_token, start_file_id=None, max_file_count=None, prefix=None
    ):
        start_file_id = start_file_id or self.FIRST_FILE_ID
        max_file_count = max_file_count or 100
        all_unfinished_ids = set(
            k for (k, v) in self.file_id_to_file.items()
            if v.action == 'start' and k <= start_file_id and
            (prefix is None or v.name.startswith(prefix))
        )
        ids_in_order = sorted(all_unfinished_ids, reverse=True)

        file_dict_list = [
            file_sim.as_start_large_file_result(account_auth_token)
            for file_sim in (
                self.file_id_to_file[file_id] for file_id in ids_in_order[:max_file_count]
            )
        ]  # yapf: disable
        next_file_id = None
        if len(file_dict_list) == max_file_count:
            next_file_id = str(int(file_dict_list[-1]['fileId']) - 1)
        return dict(files=file_dict_list, nextFileId=next_file_id)

    def start_large_file(
        self,
        account_auth_token,
        file_name,
        content_type,
        file_info,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        file_id = self._next_file_id()
        sse = server_side_encryption or self.default_server_side_encryption
        if sse:  # FIXME: remove this part when RawApi<->Encryption adapters are implemented properly
            file_info = sse.add_key_id_to_file_info(file_info)
        file_sim = self.FILE_SIMULATOR_CLASS(
            self.account_id, self, file_id, 'start', file_name, content_type, 'none',
            file_info, None, next(self.upload_timestamp_counter), server_side_encryption=sse,
            file_retention=file_retention, legal_hold=legal_hold,
        )  # yapf: disable
        self.file_id_to_file[file_id] = file_sim
        self.file_name_and_id_to_file[file_sim.sort_key()] = file_sim
        return file_sim.as_start_large_file_result(account_auth_token)

    def _update_bucket(
        self,
        bucket_type=None,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        if_revision_is: Optional[int] = None,
        default_server_side_encryption: Optional[EncryptionSetting] = None,
        default_retention: Optional[BucketRetentionSetting] = None,
    ):
        if if_revision_is is not None and self.revision != if_revision_is:
            raise Conflict()

        if bucket_type is not None:
            self.bucket_type = bucket_type
        if bucket_info is not None:
            self.bucket_info = bucket_info
        if cors_rules is not None:
            self.cors_rules = cors_rules
        if lifecycle_rules is not None:
            self.lifecycle_rules = lifecycle_rules
        if default_server_side_encryption is not None:
            self.default_server_side_encryption = default_server_side_encryption
        if default_retention:
            self.default_retention = default_retention
        self.revision += 1
        return self.bucket_dict(self.api.current_token)

    def upload_file(
        self,
        upload_id,
        upload_auth_token,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_infos,
        data_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        data_bytes = self._simulate_chunked_post(data_stream, content_length)
        assert len(data_bytes) == content_length
        if content_sha1 == HEX_DIGITS_AT_END:
            content_sha1 = data_bytes[-40:].decode()
            data_bytes = data_bytes[0:-40]
            content_length -= 40
        elif len(content_sha1) != 40:
            raise ValueError(content_sha1)
        computed_sha1 = hex_sha1_of_bytes(data_bytes)
        if content_sha1 != computed_sha1:
            raise FileSha1Mismatch(file_name)
        file_id = self._next_file_id()

        encryption = server_side_encryption or self.default_server_side_encryption
        if encryption:  # FIXME: remove this part when RawApi<->Encryption adapters are implemented properly
            file_infos = encryption.add_key_id_to_file_info(file_infos)

        file_sim = self.FILE_SIMULATOR_CLASS(
            self.account_id,
            self,
            file_id,
            'upload',
            file_name,
            content_type,
            content_sha1,
            file_infos,
            data_bytes,
            next(self.upload_timestamp_counter),
            server_side_encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )
        self.file_id_to_file[file_id] = file_sim
        self.file_name_and_id_to_file[file_sim.sort_key()] = file_sim
        return file_sim.as_upload_result(upload_auth_token)

    def upload_part(
        self,
        file_id,
        part_number,
        content_length,
        sha1_sum,
        input_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
    ):
        file_sim = self.file_id_to_file[file_id]
        part_data = self._simulate_chunked_post(input_stream, content_length)
        assert len(part_data) == content_length
        if sha1_sum == HEX_DIGITS_AT_END:
            sha1_sum = part_data[-40:].decode()
            part_data = part_data[0:-40]
            content_length -= 40
        computed_sha1 = hex_sha1_of_bytes(part_data)
        if sha1_sum != computed_sha1:
            raise PartSha1Mismatch(file_id)
        part = PartSimulator(file_sim.file_id, part_number, content_length, sha1_sum, part_data)
        file_sim.add_part(part_number, part)
        result = dict(
            fileId=file_id,
            partNumber=part_number,
            contentLength=content_length,
            contentSha1=sha1_sum,
        )  # yapf: disable
        if server_side_encryption is not None:
            result['serverSideEncryption'] = server_side_encryption.serialize_to_json_for_request()
        return result

    def _simulate_chunked_post(
        self, stream, content_length, min_chunks=4, max_chunk_size=8096, simulate_retry=True
    ):
        chunk_size = max_chunk_size
        chunks_num = self._chunks_number(content_length, chunk_size)
        if chunks_num < min_chunks:
            chunk_size = max(content_length // min_chunks, 1)
        loop_count = 2 if simulate_retry else 1
        stream_data = None
        for _ in range(loop_count):
            chunks = []
            stream.seek(0)  # we always do this in `do_post` in `b2http` so we want it here *always*
            while True:
                data = stream.read(chunk_size)
                chunks.append(data)
                if not data:
                    break
            _stream_data = b''.join(chunks)
            if stream_data is not None:
                assert _stream_data == stream_data
            stream_data = _stream_data
        return stream_data

    def _chunks_number(self, content_length, chunk_size):
        chunks_number = content_length // chunk_size
        if content_length % chunk_size > 0:
            chunks_number = chunks_number + 1
        return chunks_number

    def _next_file_id(self):
        return str(next(self.file_id_counter))


class RawSimulator(AbstractRawApi):
    """
    Implement the same interface as B2RawApi by simulating all of the
    calls and keeping state in memory.

    The intended use for this class is for unit tests that test things
    built on top of B2RawApi.
    """

    BUCKET_SIMULATOR_CLASS = BucketSimulator
    API_URL = 'http://api.example.com'
    S3_API_URL = 'http://s3.api.example.com'
    DOWNLOAD_URL = 'http://download.example.com'

    MIN_PART_SIZE = 200

    # This is the maximum duration in seconds that an application key can be valid (1000 days).
    MAX_DURATION_IN_SECONDS = 86400000

    UPLOAD_PART_MATCHER = re.compile('https://upload.example.com/part/([^/]*)')
    UPLOAD_URL_MATCHER = re.compile(r'https://upload.example.com/([^/]*)/([^/]*)')
    DOWNLOAD_URL_MATCHER = re.compile(
        DOWNLOAD_URL + '(?:' + '|'.join(
            (
                '/b2api/v[0-9]+/b2_download_file_by_id\?fileId=(?P<file_id>[^/]+)',
                '/file/(?P<bucket_name>[^/]+)/(?P<file_name>.+)',
            )
        ) + ')$'
    )  # yapf: disable

    def __init__(self):
        # Map from application_key_id to KeySimulator.
        # The entry for the master application key ID is for the master application
        # key for the account, and the entries with non-master application keys
        # are for keys created b2 createKey().
        self.key_id_to_key = dict()

        # Map from auth token to the KeySimulator for it.
        self.auth_token_to_key = dict()

        # Set of auth tokens that have expired
        self.expired_auth_tokens = set()

        # Map from auth token to a lock that upload procedure acquires
        # when utilizing the token
        self.currently_used_auth_tokens = collections.defaultdict(threading.Lock)

        # Counter for generating auth tokens.
        self.auth_token_counter = 0

        # Counter for generating account IDs an their matching master application keys.
        self.account_counter = 0

        self.bucket_name_to_bucket = dict()
        self.bucket_id_to_bucket = dict()
        self.bucket_id_counter = iter(range(100))
        self.file_id_to_bucket_id = {}
        self.all_application_keys = []
        self.app_key_counter = 0
        self.upload_errors = []

    def expire_auth_token(self, auth_token):
        """
        Simulate the auth token expiring.

        The next call that tries to use this auth token will get an
        auth_token_expired error.
        """
        assert auth_token in self.auth_token_to_key
        self.expired_auth_tokens.add(auth_token)

    def create_account(self):
        """
        Return (accountId, masterApplicationKey) for a newly created account.
        """
        # Pick the IDs for the account and the key
        account_id = 'account-%d' % (self.account_counter,)
        master_key = 'masterKey-%d' % (self.account_counter,)
        self.account_counter += 1

        # Create the key
        self.key_id_to_key[account_id] = KeySimulator(
            account_id=account_id,
            name='master',
            application_key_id=account_id,
            key=master_key,
            capabilities=ALL_CAPABILITIES,
            expiration_timestamp_or_none=None,
            bucket_id_or_none=None,
            bucket_name_or_none=None,
            name_prefix_or_none=None,
        )

        # Return the info
        return (account_id, master_key)

    def set_upload_errors(self, errors):
        """
        Store a sequence of exceptions to raise on upload.  Each one will
        be raised in turn, until they are all gone.  Then the next upload
        will succeed.
        """
        assert len(self.upload_errors) == 0
        self.upload_errors = errors

    def authorize_account(self, realm_url, application_key_id, application_key):
        key_sim = self.key_id_to_key.get(application_key_id)
        if key_sim is None:
            raise InvalidAuthToken('application key ID not valid', 'unauthorized')
        if application_key != key_sim.key:
            raise InvalidAuthToken('secret key is wrong', 'unauthorized')
        auth_token = 'auth_token_%d' % (self.auth_token_counter,)
        self.current_token = auth_token
        self.auth_token_counter += 1
        self.auth_token_to_key[auth_token] = key_sim
        allowed = key_sim.get_allowed()
        bucketId = allowed.get('bucketId')
        if (bucketId is not None) and (bucketId in self.bucket_id_to_bucket):
            allowed['bucketName'] = self.bucket_id_to_bucket[bucketId].bucket_name
        else:
            allowed['bucketName'] = None
        return dict(
            accountId=key_sim.account_id,
            authorizationToken=auth_token,
            apiUrl=self.API_URL,
            downloadUrl=self.DOWNLOAD_URL,
            recommendedPartSize=self.MIN_PART_SIZE,
            absoluteMinimumPartSize=self.MIN_PART_SIZE,
            allowed=allowed,
            s3ApiUrl=self.S3_API_URL,
        )

    def cancel_large_file(self, api_url, account_auth_token, file_id):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        return bucket.cancel_large_file(file_id)

    def create_bucket(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_name,
        bucket_type,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        default_server_side_encryption: Optional[EncryptionSetting] = None,
        is_file_lock_enabled: Optional[bool] = None,
    ):
        if not re.match(r'^[-a-zA-Z0-9]*$', bucket_name):
            raise BadJson('illegal bucket name: ' + bucket_name)
        self._assert_account_auth(api_url, account_auth_token, account_id, 'writeBuckets')
        if bucket_name in self.bucket_name_to_bucket:
            raise DuplicateBucketName(bucket_name)
        bucket_id = 'bucket_' + str(next(self.bucket_id_counter))
        bucket = self.BUCKET_SIMULATOR_CLASS(
            self,
            account_id,
            bucket_id,
            bucket_name,
            bucket_type,
            bucket_info,
            cors_rules,
            lifecycle_rules,
            # watch out for options!
            default_server_side_encryption=default_server_side_encryption,
            is_file_lock_enabled=is_file_lock_enabled,
        )
        self.bucket_name_to_bucket[bucket_name] = bucket
        self.bucket_id_to_bucket[bucket_id] = bucket
        return bucket.bucket_dict(account_auth_token)  # TODO it should be an object, right?

    def create_key(
        self,
        api_url,
        account_auth_token,
        account_id,
        capabilities,
        key_name,
        valid_duration_seconds,
        bucket_id,
        name_prefix,
    ):
        if not re.match(r'^[A-Za-z0-9-]{1,100}$', key_name):
            raise BadJson('illegal key name: ' + key_name)
        if valid_duration_seconds is not None:
            if valid_duration_seconds < 1 or valid_duration_seconds > self.MAX_DURATION_IN_SECONDS:
                raise BadJson(
                    'valid duration must be greater than 0, and less than 1000 days in seconds'
                )
        self._assert_account_auth(api_url, account_auth_token, account_id, 'writeKeys')

        if valid_duration_seconds is None:
            expiration_timestamp_or_none = None
        else:
            expiration_timestamp_or_none = int(time.time() + valid_duration_seconds)

        index = self.app_key_counter
        self.app_key_counter += 1
        application_key_id = 'appKeyId%d' % (index,)
        app_key = 'appKey%d' % (index,)
        if bucket_id is None:
            bucket_name_or_none = None
        else:
            bucket_name_or_none = self._get_bucket_by_id(bucket_id).bucket_name
        key_sim = KeySimulator(
            account_id=account_id,
            name=key_name,
            application_key_id=application_key_id,
            key=app_key,
            capabilities=capabilities,
            expiration_timestamp_or_none=expiration_timestamp_or_none,
            bucket_id_or_none=bucket_id,
            bucket_name_or_none=bucket_name_or_none,
            name_prefix_or_none=name_prefix,
        )
        self.key_id_to_key[application_key_id] = key_sim
        self.all_application_keys.append(key_sim)
        return key_sim.as_created_key()

    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'deleteFiles')
        return bucket.delete_file_version(file_id, file_name)

    def update_file_retention(
        self,
        api_url,
        account_auth_token,
        file_id,
        file_name,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        return bucket.update_file_retention(
            account_auth_token,
            file_id,
            file_name,
            file_retention,
            bypass_governance,
        )

    def update_file_legal_hold(
        self,
        api_url,
        account_auth_token,
        file_id,
        file_name,
        legal_hold: bool,
    ):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        return bucket.update_file_legal_hold(
            account_auth_token,
            file_id,
            file_name,
            legal_hold,
        )

    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        self._assert_account_auth(api_url, account_auth_token, account_id, 'deleteBuckets')
        bucket = self._get_bucket_by_id(bucket_id)
        del self.bucket_name_to_bucket[bucket.bucket_name]
        del self.bucket_id_to_bucket[bucket_id]
        return bucket.bucket_dict(account_auth_token)

    def download_file_from_url(
        self,
        account_auth_token_or_none,
        url,
        range_=None,
        encryption: Optional[EncryptionSetting] = None
    ):
        # TODO: check auth token if bucket is not public
        matcher = self.DOWNLOAD_URL_MATCHER.match(url)
        assert matcher is not None, url
        groupdict = matcher.groupdict()
        file_id = groupdict['file_id']
        bucket_name = groupdict['bucket_name']
        file_name = groupdict['file_name']
        if file_id is not None:
            bucket_id = self.file_id_to_bucket_id[file_id]
            bucket = self._get_bucket_by_id(bucket_id)
            return bucket.download_file_by_id(
                account_auth_token_or_none,
                file_id,
                range_=range_,
                url=url,
                encryption=encryption,
            )
        elif bucket_name is not None and file_name is not None:
            bucket = self._get_bucket_by_name(bucket_name)
            return bucket.download_file_by_name(
                account_auth_token_or_none,
                b2_url_decode(file_name),
                range_=range_,
                url=url,
                encryption=encryption,
            )
        else:
            assert False

    def delete_key(self, api_url, account_auth_token, application_key_id):
        assert api_url == self.API_URL
        return dict(
            accountId='accountId',
            applicationKeyId=application_key_id,
            keyName='keyName',
            capabilities=['listBuckets', 'readBuckets', 'writeBuckets']
        )

    def finish_large_file(self, api_url, account_auth_token, file_id, part_sha1_array):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        return bucket.finish_large_file(account_auth_token, file_id, part_sha1_array)

    def get_download_authorization(
        self, api_url, account_auth_token, bucket_id, file_name_prefix, valid_duration_in_seconds
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'shareFiles')
        return {
            'bucketId':
                bucket_id,
            'fileNamePrefix':
                file_name_prefix,
            'authorizationToken':
                'fake_download_auth_token_%s_%s_%d' % (
                    bucket_id,
                    b2_url_encode(file_name_prefix),
                    valid_duration_in_seconds,
                )
        }

    def get_file_info_by_id(self, api_url, account_auth_token, file_id):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        return bucket.get_file_info_by_id(account_auth_token, file_id)

    def get_file_info_by_name(self, api_url, account_auth_token, bucket_name, file_name):
        bucket = self._get_bucket_by_name(bucket_name)
        info = bucket.get_file_info_by_name(account_auth_token, file_name)
        return info

    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        return self._get_bucket_by_id(bucket_id).get_upload_url(account_auth_token)

    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        return self._get_bucket_by_id(bucket_id).get_upload_part_url(account_auth_token, file_id)

    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        response = bucket.hide_file(account_auth_token, file_name)
        self.file_id_to_bucket_id[response['fileId']] = bucket_id
        return response

    def copy_file(
        self,
        api_url,
        account_auth_token,
        source_file_id,
        new_file_name,
        bytes_range=None,
        metadata_directive=None,
        content_type=None,
        file_info=None,
        destination_bucket_id=None,
        destination_server_side_encryption=None,
        source_server_side_encryption=None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        bucket_id = self.file_id_to_bucket_id[source_file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        copy_file_sim = bucket.copy_file(
            source_file_id,
            new_file_name,
            bytes_range,
            metadata_directive,
            content_type,
            file_info,
            destination_bucket_id,
            destination_server_side_encryption=destination_server_side_encryption,
            source_server_side_encryption=source_server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

        if destination_bucket_id:
            # TODO: Handle and raise proper exception after server docs get updated
            dest_bucket = self.bucket_id_to_bucket[destination_bucket_id]
            assert dest_bucket.account_id == bucket.account_id
        else:
            dest_bucket = bucket

        dest_bucket.file_id_to_file[copy_file_sim.file_id] = copy_file_sim
        dest_bucket.file_name_and_id_to_file[copy_file_sim.sort_key()] = copy_file_sim

        return copy_file_sim.as_upload_result(account_auth_token)

    def copy_part(
        self,
        api_url,
        account_auth_token,
        source_file_id,
        large_file_id,
        part_number,
        bytes_range=None,
        destination_server_side_encryption: Optional[EncryptionSetting] = None,
        source_server_side_encryption: Optional[EncryptionSetting] = None,
    ):
        if destination_server_side_encryption is not None and destination_server_side_encryption.mode == EncryptionMode.SSE_B2:
            raise ValueError(
                'unsupported sse mode for copy_part!'
            )  # SSE-B2 is only to be marked in b2_start_large_file
        src_bucket_id = self.file_id_to_bucket_id[source_file_id]
        src_bucket = self._get_bucket_by_id(src_bucket_id)
        dest_bucket_id = self.file_id_to_bucket_id[large_file_id]
        dest_bucket = self._get_bucket_by_id(dest_bucket_id)

        self._assert_account_auth(api_url, account_auth_token, dest_bucket.account_id, 'writeFiles')

        file_sim = src_bucket.file_id_to_file[source_file_id]
        file_sim.check_encryption(source_server_side_encryption)
        data_bytes = get_bytes_range(file_sim.data_bytes, bytes_range)

        data_stream = StreamWithHash(io.BytesIO(data_bytes), len(data_bytes))
        content_length = len(data_stream)
        sha1_sum = HEX_DIGITS_AT_END

        return dest_bucket.upload_part(
            large_file_id,
            part_number,
            content_length,
            sha1_sum,
            data_stream,
            server_side_encryption=destination_server_side_encryption,
        )

    def list_buckets(
        self, api_url, account_auth_token, account_id, bucket_id=None, bucket_name=None
    ):
        # First, map the bucket name to a bucket_id, so that we can check auth.
        if bucket_name is None:
            bucket_id_for_auth = bucket_id
        else:
            bucket_id_for_auth = self._get_bucket_id_or_none_for_bucket_name(bucket_name)
        self._assert_account_auth(
            api_url, account_auth_token, account_id, 'listBuckets', bucket_id_for_auth
        )

        # Do the query
        sorted_buckets = [
            self.bucket_name_to_bucket[name] for name in sorted(self.bucket_name_to_bucket)
        ]
        bucket_list = [
            bucket.bucket_dict(account_auth_token)
            for bucket in sorted_buckets if self._bucket_matches(bucket, bucket_id, bucket_name)
        ]
        return dict(buckets=bucket_list)

    def _get_bucket_id_or_none_for_bucket_name(self, bucket_name):
        for bucket in self.bucket_name_to_bucket.values():
            if bucket.bucket_name == bucket_name:
                return bucket.bucket_id

    def _bucket_matches(self, bucket, bucket_id, bucket_name):
        return (
            (bucket_id is None or bucket.bucket_id == bucket_id) and
            (bucket_name is None or bucket.bucket_name == bucket_name)
        )

    def list_file_names(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        max_file_count=None,
        prefix=None,
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(
            api_url,
            account_auth_token,
            bucket.account_id,
            'listFiles',
            bucket_id=bucket_id,
            file_name=prefix,
        )
        return bucket.list_file_names(account_auth_token, start_file_name, max_file_count, prefix)

    def list_file_versions(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None,
        prefix=None,
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(
            api_url,
            account_auth_token,
            bucket.account_id,
            'listFiles',
            bucket_id=bucket_id,
            file_name=prefix,
        )
        return bucket.list_file_versions(
            account_auth_token,
            start_file_name,
            start_file_id,
            max_file_count,
            prefix,
        )

    def list_keys(
        self,
        api_url,
        account_auth_token,
        account_id,
        max_key_count=1000,
        start_application_key_id=None
    ):
        self._assert_account_auth(api_url, account_auth_token, account_id, 'listKeys')
        keys = map(lambda key: key.as_key(), self.all_application_keys)
        return dict(keys=keys, nextKeyId=None)

    def list_parts(self, api_url, account_auth_token, file_id, start_part_number, max_part_count):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        return bucket.list_parts(file_id, start_part_number, max_part_count)

    def list_unfinished_large_files(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_id=None,
        max_file_count=None,
        prefix=None
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(
            api_url, account_auth_token, bucket.account_id, 'listFiles', file_name=prefix
        )
        start_file_id = start_file_id or ''
        max_file_count = max_file_count or 100
        return bucket.list_unfinished_large_files(
            account_auth_token, start_file_id, max_file_count, prefix
        )

    def start_large_file(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        file_name,
        content_type,
        file_info,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeFiles')
        result = bucket.start_large_file(
            account_auth_token,
            file_name,
            content_type,
            file_info,
            server_side_encryption,
            file_retention,
            legal_hold,
        )
        self.file_id_to_bucket_id[result['fileId']] = bucket_id

        return result

    def update_bucket(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        bucket_type=None,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        if_revision_is=None,
        default_server_side_encryption: Optional[EncryptionSetting] = None,
        default_retention: Optional[BucketRetentionSetting] = None,
    ):
        assert bucket_type or bucket_info or cors_rules or lifecycle_rules or default_server_side_encryption
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id, 'writeBuckets')
        return bucket._update_bucket(
            bucket_type=bucket_type,
            bucket_info=bucket_info,
            cors_rules=cors_rules,
            lifecycle_rules=lifecycle_rules,
            if_revision_is=if_revision_is,
            default_server_side_encryption=default_server_side_encryption,
            default_retention=default_retention,
        )

    def upload_file(
        self,
        upload_url,
        upload_auth_token,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_infos,
        data_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        with ConcurrentUsedAuthTokenGuard(
            self.currently_used_auth_tokens[upload_auth_token], upload_auth_token
        ):
            assert upload_url == upload_auth_token
            url_match = self.UPLOAD_URL_MATCHER.match(upload_url)
            if url_match is None:
                raise BadUploadUrl(upload_url)
            if self.upload_errors:
                raise self.upload_errors.pop(0)
            bucket_id, upload_id = url_match.groups()
            bucket = self._get_bucket_by_id(bucket_id)
            if server_side_encryption is not None:
                assert server_side_encryption.mode in (
                    EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
                )
                file_infos = server_side_encryption.add_key_id_to_file_info(file_infos)
            response = bucket.upload_file(
                upload_id,
                upload_auth_token,
                file_name,
                content_length,
                content_type,
                content_sha1,
                file_infos,
                data_stream,
                server_side_encryption,
                file_retention,
                legal_hold,
            )
            file_id = response['fileId']
            self.file_id_to_bucket_id[file_id] = bucket_id

        return response

    def upload_part(
        self,
        upload_url,
        upload_auth_token,
        part_number,
        content_length,
        sha1_sum,
        input_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
    ):
        with ConcurrentUsedAuthTokenGuard(
            self.currently_used_auth_tokens[upload_auth_token], upload_auth_token
        ):
            url_match = self.UPLOAD_PART_MATCHER.match(upload_url)
            if url_match is None:
                raise BadUploadUrl(upload_url)
            file_id = url_match.group(1)
            bucket_id = self.file_id_to_bucket_id[file_id]
            bucket = self._get_bucket_by_id(bucket_id)
            part = bucket.upload_part(
                file_id, part_number, content_length, sha1_sum, input_stream, server_side_encryption
            )
        return part

    def _assert_account_auth(
        self, api_url, account_auth_token, account_id, capability, bucket_id=None, file_name=None
    ):
        key_sim = self.auth_token_to_key.get(account_auth_token)
        assert key_sim is not None
        assert api_url == self.API_URL
        assert account_id == key_sim.account_id
        if account_auth_token in self.expired_auth_tokens:
            raise InvalidAuthToken('auth token expired', 'auth_token_expired')
        if capability not in key_sim.capabilities:
            raise Unauthorized('', 'unauthorized')
        if key_sim.bucket_id_or_none is not None and key_sim.bucket_id_or_none != bucket_id:
            raise Unauthorized('', 'unauthorized')
        if key_sim.name_prefix_or_none is not None:
            if file_name is not None and not file_name.startswith(key_sim.name_prefix_or_none):
                raise Unauthorized('', 'unauthorized')

    def _get_bucket_by_id(self, bucket_id):
        if bucket_id not in self.bucket_id_to_bucket:
            raise NonExistentBucket(bucket_id)
        return self.bucket_id_to_bucket[bucket_id]

    def _get_bucket_by_name(self, bucket_name):
        if bucket_name not in self.bucket_name_to_bucket:
            raise NonExistentBucket(bucket_name)
        return self.bucket_name_to_bucket[bucket_name]
