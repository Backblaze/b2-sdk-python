######################################################################
#
# File: b2sdk/raw_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import base64
import io
import os
import random
import re
import sys
import time
import traceback
from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from logging import getLogger
from typing import Any, Dict, Optional

from .b2http import B2Http
from .exception import FileOrBucketNotFound, ResourceNotFound, UnusableFileName, InvalidMetadataDirective, WrongEncryptionModeForBucketDefault, AccessDenied, SSECKeyError, RetentionWriteError
from .encryption.setting import EncryptionAlgorithm, EncryptionMode, EncryptionSetting
from .file_lock import BucketRetentionSetting, FileRetentionSetting, NO_RETENTION_FILE_SETTING, RetentionMode, RetentionPeriod, LegalHold
from .utils import b2_url_encode, hex_sha1_of_stream

# All supported realms
REALM_URLS = {
    'production': 'https://api.backblazeb2.com',
    'dev': 'http://api.backblazeb2.xyz:8180',
    'staging': 'https://api.backblaze.net',
}

# All possible capabilities
ALL_CAPABILITIES = [
    'listKeys',
    'writeKeys',
    'deleteKeys',
    'listBuckets',
    'writeBuckets',
    'deleteBuckets',
    'readBucketEncryption',
    'writeBucketEncryption',
    'readBucketRetentions',
    'writeBucketRetentions',
    'writeFileRetentions',
    'writeFileLegalHolds',
    'readFileRetentions',
    'readFileLegalHolds',
    'listFiles',
    'readFiles',
    'shareFiles',
    'writeFiles',
    'deleteFiles',
]

# Standard names for file info entries
SRC_LAST_MODIFIED_MILLIS = 'src_last_modified_millis'

# Special X-Bz-Content-Sha1 value to verify checksum at the end
HEX_DIGITS_AT_END = 'hex_digits_at_end'

# API version number to use when calling the service
API_VERSION = 'v2'

logger = getLogger(__name__)


@unique
class MetadataDirectiveMode(Enum):
    """ Mode of handling metadata when copying a file """
    COPY = 401  #: copy metadata from the source file
    REPLACE = 402  #: ignore the source file metadata and set it to provided values


class AbstractRawApi(metaclass=ABCMeta):
    """
    Direct access to the B2 web apis.
    """

    @abstractmethod
    def authorize_account(self, realm_url, application_key_id, application_key):
        pass

    @abstractmethod
    def cancel_large_file(self, api_url, account_auth_token, file_id):
        pass

    @abstractmethod
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
        destination_server_side_encryption: Optional[EncryptionSetting] = None,
        source_server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def create_key(
        self, api_url, account_auth_token, account_id, capabilities, key_name,
        valid_duration_seconds, bucket_id, name_prefix
    ):
        pass

    @abstractmethod
    def download_file_from_url(
        self,
        account_auth_token_or_none,
        url,
        range_=None,
        encryption: Optional[EncryptionSetting] = None,
    ):
        pass

    @abstractmethod
    def delete_key(self, api_url, account_auth_token, application_key_id):
        pass

    @abstractmethod
    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        pass

    @abstractmethod
    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        pass

    @abstractmethod
    def finish_large_file(self, api_url, account_auth_token, file_id, part_sha1_array):
        pass

    @abstractmethod
    def get_download_authorization(
        self, api_url, account_auth_token, bucket_id, file_name_prefix, valid_duration_in_seconds
    ):
        pass

    @abstractmethod
    def get_file_info_by_id(self, api_url: str, account_auth_token: str,
                            file_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_file_info_by_name(
        self, download_url: str, account_auth_token: str, bucket_name: str, file_name: str
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        pass

    @abstractmethod
    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        pass

    @abstractmethod
    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        pass

    @abstractmethod
    def list_buckets(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_id=None,
        bucket_name=None,
    ):
        pass

    @abstractmethod
    def list_file_names(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        max_file_count=None,
        prefix=None,
    ):
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def list_keys(
        self,
        api_url,
        account_auth_token,
        account_id,
        max_key_count=None,
        start_application_key_id=None
    ):
        pass

    @abstractmethod
    def list_parts(self, api_url, account_auth_token, file_id, start_part_number, max_part_count):
        pass

    @abstractmethod
    def list_unfinished_large_files(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_id=None,
        max_file_count=None,
        prefix=None,
    ):
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def update_file_retention(
        self,
        api_url,
        account_auth_token,
        file_id,
        file_name,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ):
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    def get_download_url_by_id(self, download_url, file_id):
        return '%s/b2api/%s/b2_download_file_by_id?fileId=%s' % (download_url, API_VERSION, file_id)

    def get_download_url_by_name(self, download_url, bucket_name, file_name):
        return download_url + '/file/' + bucket_name + '/' + b2_url_encode(file_name)


class B2RawApi(AbstractRawApi):
    """
    Provide access to the B2 web APIs, exactly as they are provided by b2.

    Requires that you provide all necessary URLs and auth tokens for each call.

    Each API call decodes the returned JSON and returns a dict.

    For details on what each method does, see the B2 docs:
        https://www.backblaze.com/b2/docs/

    This class is intended to be a super-simple, very thin layer on top
    of the HTTP calls.  It can be mocked-out for testing higher layers.
    And this class can be tested by exercising each call just once,
    which is relatively quick.
    """

    def __init__(self, b2_http):
        self.b2_http = b2_http

    def _post_json(self, base_url, api_name, auth, **params) -> Dict[str, Any]:
        """
        A helper method for calling an API with the given auth and params.

        :param base_url: something like "https://api001.backblazeb2.com/"
        :param auth: passed in Authorization header
        :param api_name: example: "b2_create_bucket"
        :param args: the rest of the parameters are passed to b2
        :return: the decoded JSON response
        :rtype: dict
        """
        url = '%s/b2api/%s/%s' % (base_url, API_VERSION, api_name)
        headers = {'Authorization': auth}
        return self.b2_http.post_json_return_json(url, headers, params)

    def authorize_account(self, realm_url, application_key_id, application_key):
        auth = b'Basic ' + base64.b64encode(
            ('%s:%s' % (application_key_id, application_key)).encode()
        )
        return self._post_json(realm_url, 'b2_authorize_account', auth)

    def cancel_large_file(self, api_url, account_auth_token, file_id):
        return self._post_json(api_url, 'b2_cancel_large_file', account_auth_token, fileId=file_id)

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
        kwargs = dict(
            accountId=account_id,
            bucketName=bucket_name,
            bucketType=bucket_type,
        )
        if bucket_info is not None:
            kwargs['bucketInfo'] = bucket_info
        if cors_rules is not None:
            kwargs['corsRules'] = cors_rules
        if lifecycle_rules is not None:
            kwargs['lifecycleRules'] = lifecycle_rules
        if default_server_side_encryption is not None:
            if not default_server_side_encryption.mode.can_be_set_as_bucket_default():
                raise WrongEncryptionModeForBucketDefault(default_server_side_encryption.mode)
            kwargs['defaultServerSideEncryption'
                  ] = default_server_side_encryption.serialize_to_json_for_request()
        if is_file_lock_enabled is not None:
            kwargs['fileLockEnabled'] = is_file_lock_enabled
        return self._post_json(
            api_url,
            'b2_create_bucket',
            account_auth_token,
            **kwargs,
        )

    def create_key(
        self, api_url, account_auth_token, account_id, capabilities, key_name,
        valid_duration_seconds, bucket_id, name_prefix
    ):
        return self._post_json(
            api_url,
            'b2_create_key',
            account_auth_token,
            accountId=account_id,
            capabilities=capabilities,
            keyName=key_name,
            validDurationInSeconds=valid_duration_seconds,
            bucketId=bucket_id,
            namePrefix=name_prefix,
        )

    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        return self._post_json(
            api_url,
            'b2_delete_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id
        )

    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        return self._post_json(
            api_url,
            'b2_delete_file_version',
            account_auth_token,
            fileId=file_id,
            fileName=file_name
        )

    def delete_key(self, api_url, account_auth_token, application_key_id):
        return self._post_json(
            api_url,
            'b2_delete_key',
            account_auth_token,
            applicationKeyId=application_key_id,
        )

    def download_file_from_url(
        self,
        account_auth_token_or_none,
        url,
        range_=None,
        encryption: Optional[EncryptionSetting] = None,
    ):
        """
        Issue a streaming request for download of a file, potentially authorized.

        :param str account_auth_token_or_none: an optional account auth token to pass in
        :param str url: the full URL to download from
        :param tuple range: two-element tuple for http Range header
        :param b2sdk.v1.EncryptionSetting encryption: encryption settings for downloading
        :return: b2_http response
        """
        request_headers = {}
        _add_range_header(request_headers, range_)

        if encryption is not None:
            assert encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            encryption.add_to_download_headers(request_headers)

        if account_auth_token_or_none is not None:
            request_headers['Authorization'] = account_auth_token_or_none
        try:
            return self.b2_http.get_content(url, request_headers)
        except AccessDenied:
            raise SSECKeyError()

    def finish_large_file(self, api_url, account_auth_token, file_id, part_sha1_array):
        return self._post_json(
            api_url,
            'b2_finish_large_file',
            account_auth_token,
            fileId=file_id,
            partSha1Array=part_sha1_array
        )

    def get_download_authorization(
        self, api_url, account_auth_token, bucket_id, file_name_prefix, valid_duration_in_seconds
    ):
        return self._post_json(
            api_url,
            'b2_get_download_authorization',
            account_auth_token,
            bucketId=bucket_id,
            fileNamePrefix=file_name_prefix,
            validDurationInSeconds=valid_duration_in_seconds
        )

    def get_file_info_by_id(self, api_url: str, account_auth_token: str,
                            file_id: str) -> Dict[str, Any]:
        return self._post_json(api_url, 'b2_get_file_info', account_auth_token, fileId=file_id)

    def get_file_info_by_name(
        self, download_url: str, account_auth_token: str, bucket_name: str, file_name: str
    ) -> Dict[str, Any]:
        download_url = self.get_download_url_by_name(download_url, bucket_name, file_name)
        try:
            response = self.b2_http.head_content(
                download_url, headers={"Authorization": account_auth_token}
            )
            return response.headers
        except ResourceNotFound:
            logger.debug("Resource Not Found: %s" % download_url)
            raise FileOrBucketNotFound(bucket_name, file_name)

    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        return self._post_json(api_url, 'b2_get_upload_url', account_auth_token, bucketId=bucket_id)

    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        return self._post_json(
            api_url, 'b2_get_upload_part_url', account_auth_token, fileId=file_id
        )

    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        return self._post_json(
            api_url, 'b2_hide_file', account_auth_token, bucketId=bucket_id, fileName=file_name
        )

    def list_buckets(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_id=None,
        bucket_name=None,
    ):
        return self._post_json(
            api_url,
            'b2_list_buckets',
            account_auth_token,
            accountId=account_id,
            bucketTypes=['all'],
            bucketId=bucket_id,
            bucketName=bucket_name,
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
        return self._post_json(
            api_url,
            'b2_list_file_names',
            account_auth_token,
            bucketId=bucket_id,
            startFileName=start_file_name,
            maxFileCount=max_file_count,
            prefix=prefix,
        )

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
        return self._post_json(
            api_url,
            'b2_list_file_versions',
            account_auth_token,
            bucketId=bucket_id,
            startFileName=start_file_name,
            startFileId=start_file_id,
            maxFileCount=max_file_count,
            prefix=prefix,
        )

    def list_keys(
        self,
        api_url,
        account_auth_token,
        account_id,
        max_key_count=None,
        start_application_key_id=None
    ):
        return self._post_json(
            api_url,
            'b2_list_keys',
            account_auth_token,
            accountId=account_id,
            maxKeyCount=max_key_count,
            startApplicationKeyId=start_application_key_id,
        )

    def list_parts(self, api_url, account_auth_token, file_id, start_part_number, max_part_count):
        return self._post_json(
            api_url,
            'b2_list_parts',
            account_auth_token,
            fileId=file_id,
            startPartNumber=start_part_number,
            maxPartCount=max_part_count
        )

    def list_unfinished_large_files(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_id=None,
        max_file_count=None,
        prefix=None,
    ):
        return self._post_json(
            api_url,
            'b2_list_unfinished_large_files',
            account_auth_token,
            bucketId=bucket_id,
            startFileId=start_file_id,
            maxFileCount=max_file_count,
            namePrefix=prefix,
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
        kwargs = {}
        if server_side_encryption is not None:
            assert server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            kwargs['serverSideEncryption'] = server_side_encryption.serialize_to_json_for_request()

        if legal_hold is not None:
            kwargs['legalHold'] = legal_hold.to_server()

        if file_retention is not None:
            kwargs['fileRetention'] = file_retention.serialize_to_json_for_request()

        return self._post_json(
            api_url,
            'b2_start_large_file',
            account_auth_token,
            bucketId=bucket_id,
            fileName=file_name,
            fileInfo=file_info,
            contentType=content_type,
            **kwargs
        )

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
        assert bucket_info is not None or bucket_type is not None

        kwargs = {}
        if if_revision_is is not None:
            kwargs['ifRevisionIs'] = if_revision_is
        if bucket_info is not None:
            kwargs['bucketInfo'] = bucket_info
        if bucket_type is not None:
            kwargs['bucketType'] = bucket_type
        if cors_rules is not None:
            kwargs['corsRules'] = cors_rules
        if lifecycle_rules is not None:
            kwargs['lifecycleRules'] = lifecycle_rules
        if default_server_side_encryption is not None:
            if not default_server_side_encryption.mode.can_be_set_as_bucket_default():
                raise WrongEncryptionModeForBucketDefault(default_server_side_encryption.mode)
            kwargs['defaultServerSideEncryption'
                  ] = default_server_side_encryption.serialize_to_json_for_request()
        if default_retention is not None:
            kwargs['defaultRetention'] = default_retention.serialize_to_json_for_request()

        return self._post_json(
            api_url,
            'b2_update_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id,
            **kwargs
        )

    def update_file_retention(
        self,
        api_url,
        account_auth_token,
        file_id,
        file_name,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ):
        kwargs = {}
        kwargs['fileRetention'] = file_retention.serialize_to_json_for_request()
        try:
            return self._post_json(
                api_url,
                'b2_update_file_retention',
                account_auth_token,
                fileId=file_id,
                fileName=file_name,
                bypassGovernance=bypass_governance,
                **kwargs
            )
        except AccessDenied:
            raise RetentionWriteError()

    def update_file_legal_hold(
        self,
        api_url,
        account_auth_token,
        file_id,
        file_name,
        legal_hold: LegalHold,
    ):
        try:
            return self._post_json(
                api_url,
                'b2_update_file_legal_hold',
                account_auth_token,
                fileId=file_id,
                fileName=file_name,
                legalHold=legal_hold.to_server(),
            )
        except AccessDenied:
            raise RetentionWriteError()

    def unprintable_to_hex(self, string):
        """
        Replace unprintable chars in string with a hex representation.

        :param string: an arbitrary string, possibly with unprintable characters.
        :return: the string, with unprintable characters changed to hex (e.g., "\x07")

        """
        unprintables_pattern = re.compile(r'[\x00-\x1f]')

        def hexify(match):
            return r'\x{0:02x}'.format(ord(match.group()))

        return unprintables_pattern.sub(hexify, string)

    def check_b2_filename(self, filename):
        """
        Raise an appropriate exception with details if the filename is unusable.

        See https://www.backblaze.com/b2/docs/files.html for the rules.

        :param filename: a proposed filename in unicode
        :return: None if the filename is usable
        """
        encoded_name = filename.encode('utf-8')
        length_in_bytes = len(encoded_name)
        if length_in_bytes < 1:
            raise UnusableFileName("Filename must be at least 1 character.")
        if length_in_bytes > 1024:
            raise UnusableFileName("Filename is too long (can be at most 1024 bytes).")
        lowest_unicode_value = ord(min(filename))
        if lowest_unicode_value < 32:
            message = u"Filename \"{0}\" contains code {1} (hex {2:02x}), less than 32.".format(
                self.unprintable_to_hex(filename), lowest_unicode_value, lowest_unicode_value
            )
            raise UnusableFileName(message)
        # No DEL for you.
        if '\x7f' in filename:
            raise UnusableFileName("DEL character (0x7f) not allowed.")
        if filename[0] == '/' or filename[-1] == '/':
            raise UnusableFileName("Filename may not start or end with '/'.")
        if '//' in filename:
            raise UnusableFileName("Filename may not contain \"//\".")
        long_segment = max([len(segment.encode('utf-8')) for segment in filename.split('/')])
        if long_segment > 250:
            raise UnusableFileName("Filename segment too long (maximum 250 bytes in utf-8).")

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
        """
        Upload one, small file to b2.

        :param upload_url: the upload_url from b2_authorize_account
        :param upload_auth_token: the auth token from b2_authorize_account
        :param file_name: the name of the B2 file
        :param content_length: number of bytes in the file
        :param content_type: MIME type
        :param content_sha1: hex SHA1 of the contents of the file
        :param file_infos: extra file info to upload
        :param data_stream: a file like object from which the contents of the file can be read
        :return:
        """
        # Raise UnusableFileName if the file_name doesn't meet the rules.
        self.check_b2_filename(file_name)
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-File-Name': b2_url_encode(file_name),
            'Content-Type': content_type,
            'X-Bz-Content-Sha1': content_sha1,
        }
        for k, v in file_infos.items():
            headers['X-Bz-Info-' + k] = b2_url_encode(v)
        if server_side_encryption is not None:
            assert server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            server_side_encryption.add_to_upload_headers(headers)

        if legal_hold is not None:
            legal_hold.add_to_upload_headers(headers)

        if file_retention is not None:
            file_retention.add_to_to_upload_headers(headers)

        return self.b2_http.post_content_return_json(upload_url, headers, data_stream)

    def upload_part(
        self,
        upload_url,
        upload_auth_token,
        part_number,
        content_length,
        content_sha1,
        data_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
    ):
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-Part-Number': str(part_number),
            'X-Bz-Content-Sha1': content_sha1
        }
        if server_side_encryption is not None:
            assert server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            server_side_encryption.add_to_upload_headers(headers)

        return self.b2_http.post_content_return_json(upload_url, headers, data_stream)

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
        destination_server_side_encryption: Optional[EncryptionSetting] = None,
        source_server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        kwargs = {}
        if bytes_range is not None:
            range_dict = {}
            _add_range_header(range_dict, bytes_range)
            kwargs['range'] = range_dict['Range']

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
            kwargs['metadataDirective'] = metadata_directive.name

        if content_type is not None:
            kwargs['contentType'] = content_type
        if file_info is not None:
            kwargs['fileInfo'] = file_info
        if destination_bucket_id is not None:
            kwargs['destinationBucketId'] = destination_bucket_id
        if destination_server_side_encryption is not None:
            assert destination_server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            kwargs['destinationServerSideEncryption'
                  ] = destination_server_side_encryption.serialize_to_json_for_request()
        if source_server_side_encryption is not None:
            assert source_server_side_encryption.mode == EncryptionMode.SSE_C
            kwargs['sourceServerSideEncryption'
                  ] = source_server_side_encryption.serialize_to_json_for_request()

        if legal_hold is not None:
            kwargs['legalHold'] = legal_hold.to_server()

        if file_retention is not None:
            kwargs['fileRetention'] = file_retention.serialize_to_json_for_request()

        try:
            return self._post_json(
                api_url,
                'b2_copy_file',
                account_auth_token,
                sourceFileId=source_file_id,
                fileName=new_file_name,
                **kwargs
            )
        except AccessDenied:
            raise SSECKeyError()

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
        kwargs = {}
        if bytes_range is not None:
            range_dict = {}
            _add_range_header(range_dict, bytes_range)
            kwargs['range'] = range_dict['Range']
        if destination_server_side_encryption is not None:
            assert destination_server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            kwargs['destinationServerSideEncryption'
                  ] = destination_server_side_encryption.serialize_to_json_for_request()
        if source_server_side_encryption is not None:
            assert source_server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            kwargs['sourceServerSideEncryption'
                  ] = source_server_side_encryption.serialize_to_json_for_request()
        try:
            return self._post_json(
                api_url,
                'b2_copy_part',
                account_auth_token,
                sourceFileId=source_file_id,
                largeFileId=large_file_id,
                partNumber=part_number,
                **kwargs
            )
        except AccessDenied:
            raise SSECKeyError()


def test_raw_api():
    """
    Exercise the code in B2RawApi by making each call once, just
    to make sure the parameters are passed in, and the result is
    passed back.

    The goal is to be a complete test of B2RawApi, so the tests for
    the rest of the code can use the simulator.

    Prints to stdout if things go wrong.

    :return: 0 on success, non-zero on failure
    """
    try:
        raw_api = B2RawApi(B2Http())
        test_raw_api_helper(raw_api)
        return 0
    except Exception:
        traceback.print_exc(file=sys.stdout)
        return 1


def test_raw_api_helper(raw_api):
    """
    Try each of the calls to the raw api.  Raise an
    exception if anything goes wrong.

    This uses a Backblaze account that is just for this test.
    The account uses the free level of service, which should
    be enough to run this test a reasonable number of times
    each day.  If somebody abuses the account for other things,
    this test will break and we'll have to do something about
    it.
    """
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        print('B2_TEST_APPLICATION_KEY_ID is not set.', file=sys.stderr)
        sys.exit(1)

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        print('B2_TEST_APPLICATION_KEY is not set.', file=sys.stderr)
        sys.exit(1)

    realm = os.environ.get('B2_TEST_ENVIRONMENT', 'production')
    realm_url = REALM_URLS.get(realm, realm)

    # b2_authorize_account
    print('b2_authorize_account')
    auth_dict = raw_api.authorize_account(realm_url, application_key_id, application_key)
    account_id = auth_dict['accountId']
    account_auth_token = auth_dict['authorizationToken']
    api_url = auth_dict['apiUrl']
    download_url = auth_dict['downloadUrl']

    # b2_create_key
    print('b2_create_key')
    key_dict = raw_api.create_key(
        api_url,
        account_auth_token,
        account_id,
        ['readFiles'],
        'testKey',
        None,
        None,
        None,
    )

    # b2_list_keys
    print('b2_list_keys')
    raw_api.list_keys(api_url, account_auth_token, account_id, 10)

    # b2_delete_key
    print('b2_delete_key')
    raw_api.delete_key(api_url, account_auth_token, key_dict['applicationKeyId'])

    # b2_create_bucket, with a unique bucket name
    # Include the account ID in the bucket name to be
    # sure it doesn't collide with bucket names from
    # other accounts.
    print('b2_create_bucket')
    bucket_name = 'test-raw-api-%s-%d-%d' % (
        account_id, int(time.time()), random.randint(1000, 9999)
    )

    # very verbose http debug
    #import http.client; http.client.HTTPConnection.debuglevel = 1

    bucket_dict = raw_api.create_bucket(
        api_url,
        account_auth_token,
        account_id,
        bucket_name,
        'allPublic',
        is_file_lock_enabled=True,
    )
    bucket_id = bucket_dict['bucketId']
    first_bucket_revision = bucket_dict['revision']

    ##################
    print('b2_update_bucket')
    sse_b2_aes = EncryptionSetting(
        mode=EncryptionMode.SSE_B2,
        algorithm=EncryptionAlgorithm.AES256,
    )
    sse_none = EncryptionSetting(mode=EncryptionMode.NONE)
    for encryption_setting, default_retention in [
        (
            sse_none,
            BucketRetentionSetting(mode=RetentionMode.GOVERNANCE, period=RetentionPeriod(days=1))
        ),
        (sse_b2_aes, None),
        (sse_b2_aes, BucketRetentionSetting(RetentionMode.NONE)),
    ]:
        bucket_dict = raw_api.update_bucket(
            api_url,
            account_auth_token,
            account_id,
            bucket_id,
            'allPublic',
            default_server_side_encryption=encryption_setting,
            default_retention=default_retention,
        )

    # b2_list_buckets
    print('b2_list_buckets')
    bucket_list_dict = raw_api.list_buckets(api_url, account_auth_token, account_id)
    #print(bucket_list_dict)

    # b2_get_upload_url
    print('b2_get_upload_url')
    upload_url_dict = raw_api.get_upload_url(api_url, account_auth_token, bucket_id)
    upload_url = upload_url_dict['uploadUrl']
    upload_auth_token = upload_url_dict['authorizationToken']

    # b2_upload_file
    print('b2_upload_file')
    file_name = 'test.txt'
    file_contents = b'hello world'
    file_sha1 = hex_sha1_of_stream(io.BytesIO(file_contents), len(file_contents))
    file_dict = raw_api.upload_file(
        upload_url,
        upload_auth_token,
        file_name,
        len(file_contents),
        'text/plain',
        file_sha1,
        {'color': 'blue'},
        io.BytesIO(file_contents),
        server_side_encryption=sse_b2_aes,
    )

    file_id = file_dict['fileId']

    # b2_list_file_versions
    print('b2_list_file_versions')
    list_versions_dict = raw_api.list_file_versions(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in list_versions_dict['files']]

    # b2_download_file_by_id with auth
    print('b2_download_file_by_id (auth)')
    url = raw_api.get_download_url_by_id(download_url, file_id)
    with raw_api.download_file_from_url(account_auth_token, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_download_file_by_id no auth
    print('b2_download_file_by_id (no auth)')
    url = raw_api.get_download_url_by_id(download_url, file_id)
    with raw_api.download_file_from_url(None, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_download_file_by_name with auth
    print('b2_download_file_by_name (auth)')
    url = raw_api.get_download_url_by_name(download_url, bucket_name, file_name)
    with raw_api.download_file_from_url(account_auth_token, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_download_file_by_name no auth
    print('b2_download_file_by_name (no auth)')
    url = raw_api.get_download_url_by_name(download_url, bucket_name, file_name)
    with raw_api.download_file_from_url(None, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_get_download_authorization
    print('b2_get_download_authorization')
    download_auth = raw_api.get_download_authorization(
        api_url, account_auth_token, bucket_id, file_name[:-2], 12345
    )
    download_auth_token = download_auth['authorizationToken']

    # b2_download_file_by_name with download auth
    print('b2_download_file_by_name (download auth)')
    url = raw_api.get_download_url_by_name(download_url, bucket_name, file_name)
    with raw_api.download_file_from_url(download_auth_token, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_list_file_names
    print('b2_list_file_names')
    list_names_dict = raw_api.list_file_names(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in list_names_dict['files']]

    # b2_list_file_names (start, count)
    print('b2_list_file_names (start, count)')
    list_names_dict = raw_api.list_file_names(
        api_url, account_auth_token, bucket_id, start_file_name=file_name, max_file_count=5
    )
    assert [file_name] == [f_dict['fileName'] for f_dict in list_names_dict['files']]

    # b2_copy_file
    print('b2_copy_file')
    copy_file_name = 'test_copy.txt'
    raw_api.copy_file(api_url, account_auth_token, file_id, copy_file_name)

    # b2_get_file_info_by_id
    print('b2_get_file_info_by_id')
    file_info_dict = raw_api.get_file_info_by_id(api_url, account_auth_token, file_id)
    assert file_info_dict['fileName'] == file_name

    # b2_get_file_info_by_name
    print('b2_get_file_info_by_name (no auth)')
    info_headers = raw_api.get_file_info_by_name(download_url, None, bucket_name, file_name)
    assert info_headers['x-bz-file-id'] == file_id

    # b2_get_file_info_by_name
    print('b2_get_file_info_by_name (auth)')
    info_headers = raw_api.get_file_info_by_name(
        download_url, account_auth_token, bucket_name, file_name
    )
    assert info_headers['x-bz-file-id'] == file_id

    # b2_get_file_info_by_name
    print('b2_get_file_info_by_name (download auth)')
    info_headers = raw_api.get_file_info_by_name(
        download_url, download_auth_token, bucket_name, file_name
    )
    assert info_headers['x-bz-file-id'] == file_id

    # b2_hide_file
    print('b2_hide_file')
    raw_api.hide_file(api_url, account_auth_token, bucket_id, file_name)

    # b2_start_large_file
    print('b2_start_large_file')
    file_info = {'color': 'red'}
    large_info = raw_api.start_large_file(
        api_url,
        account_auth_token,
        bucket_id,
        file_name,
        'text/plain',
        file_info,
        server_side_encryption=sse_b2_aes,
    )
    large_file_id = large_info['fileId']

    # b2_get_upload_part_url
    print('b2_get_upload_part_url')
    upload_part_dict = raw_api.get_upload_part_url(api_url, account_auth_token, large_file_id)
    upload_part_url = upload_part_dict['uploadUrl']
    upload_path_auth = upload_part_dict['authorizationToken']

    # b2_upload_part
    print('b2_upload_part')
    part_contents = b'hello part'
    part_sha1 = hex_sha1_of_stream(io.BytesIO(part_contents), len(part_contents))
    raw_api.upload_part(
        upload_part_url, upload_path_auth, 1, len(part_contents), part_sha1,
        io.BytesIO(part_contents)
    )

    # b2_copy_part
    print('b2_copy_part')
    raw_api.copy_part(api_url, account_auth_token, file_id, large_file_id, 2, (0, 5))

    # b2_list_parts
    print('b2_list_parts')
    parts_response = raw_api.list_parts(api_url, account_auth_token, large_file_id, 1, 100)
    assert [1, 2] == [part['partNumber'] for part in parts_response['parts']]

    # b2_list_unfinished_large_files
    unfinished_list = raw_api.list_unfinished_large_files(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in unfinished_list['files']]
    assert file_info == unfinished_list['files'][0]['fileInfo']

    # b2_finish_large_file
    print('b2_finish_large_file')
    try:
        raw_api.finish_large_file(api_url, account_auth_token, large_file_id, [part_sha1])
        raise Exception('finish should have failed')
    except Exception as e:
        assert 'large files must have at least 2 parts' in str(e)
    # TODO: make another attempt to finish but this time successfully

    # b2_update_bucket
    print('b2_update_bucket')
    updated_bucket = raw_api.update_bucket(
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        'allPrivate',
        bucket_info={'color': 'blue'},
        default_retention=BucketRetentionSetting(
            mode=RetentionMode.GOVERNANCE, period=RetentionPeriod(days=1)
        ),
    )
    assert first_bucket_revision < updated_bucket['revision']

    # Clean up this test.
    _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id)

    # Clean up from old tests. Empty and delete any buckets more than an hour old.
    for bucket_dict in bucket_list_dict['buckets']:
        bucket_id = bucket_dict['bucketId']
        bucket_name = bucket_dict['bucketName']
        if _should_delete_bucket(bucket_name):
            print('cleaning up old bucket: ' + bucket_name)
            _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id)


def _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id):
    # Delete the files. This test never creates more than a few files,
    # so one call to list_file_versions should get them all.
    versions_dict = raw_api.list_file_versions(api_url, account_auth_token, bucket_id)
    for version_dict in versions_dict['files']:
        file_id = version_dict['fileId']
        file_name = version_dict['fileName']
        action = version_dict['action']
        if action in ['hide', 'upload']:
            print('b2_delete_file', file_name, action)
            if action == 'upload' and version_dict[
                'fileRetention'] and version_dict['fileRetention']['value']['mode'] is not None:
                raw_api.update_file_retention(
                    api_url,
                    account_auth_token,
                    file_id,
                    file_name,
                    NO_RETENTION_FILE_SETTING,
                    bypass_governance=True
                )
            raw_api.delete_file_version(api_url, account_auth_token, file_id, file_name)
        else:
            print('b2_cancel_large_file', file_name)
            raw_api.cancel_large_file(api_url, account_auth_token, file_id)

    # Delete the bucket
    print('b2_delete_bucket', bucket_id)
    raw_api.delete_bucket(api_url, account_auth_token, account_id, bucket_id)


def _should_delete_bucket(bucket_name):
    # Bucket names for this test look like: c7b22d0b0ad7-1460060364-5670
    # Other buckets should not be deleted.
    match = re.match(r'^test-raw-api-[a-f0-9]+-([0-9]+)-([0-9]+)', bucket_name)
    if match is None:
        return False

    # Is it more than an hour old?
    bucket_time = int(match.group(1))
    now = time.time()
    return bucket_time + 3600 <= now


def _add_range_header(headers, range_):
    if range_ is not None:
        assert len(range_) == 2, range_
        assert (range_[0] + 0) <= (range_[1] + 0), range_  # not strings
        assert range_[0] >= 0, range_
        headers['Range'] = "bytes=%d-%d" % range_


if __name__ == '__main__':
    test_raw_api()
