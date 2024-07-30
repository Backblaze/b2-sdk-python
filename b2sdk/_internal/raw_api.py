######################################################################
#
# File: b2sdk/_internal/raw_api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import base64
import functools
import warnings
from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from logging import getLogger
from typing import Any, Iterable

from .utils.escape import unprintable_to_hex
from .utils.typing import JSON
from .version_utils import FeaturePreviewWarning

try:
    from typing_extensions import Literal, NotRequired, TypedDict
except ImportError:
    from typing import Literal, NotRequired, TypedDict

from .encryption.setting import EncryptionMode, EncryptionSetting
from .exception import (
    AccessDenied,
    FileOrBucketNotFound,
    InvalidMetadataDirective,
    ResourceNotFound,
    RetentionWriteError,
    SSECKeyError,
    UnusableFileName,
    WrongEncryptionModeForBucketDefault,
)
from .file_lock import BucketRetentionSetting, FileRetentionSetting, LegalHold
from .http_constants import FILE_INFO_HEADER_PREFIX
from .replication.setting import ReplicationConfiguration
from .types import NotRequired, PositiveInt, TypedDict
from .utils import b2_url_encode
from .utils.docs import ensure_b2sdk_doc_urls

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
    'listAllBucketNames',
    'readBuckets',
    'writeBuckets',
    'deleteBuckets',
    'readBucketEncryption',
    'writeBucketEncryption',
    'readBucketRetentions',
    'writeBucketRetentions',
    'readFileRetentions',
    'writeFileRetentions',
    'readFileLegalHolds',
    'writeFileLegalHolds',
    'readBucketReplications',
    'writeBucketReplications',
    'bypassGovernance',
    'listFiles',
    'readFiles',
    'shareFiles',
    'writeFiles',
    'deleteFiles',
    'readBucketNotifications',
    'writeBucketNotifications',
]

# API version number to use when calling the service
API_VERSION = 'v2'

logger = getLogger(__name__)


@unique
class MetadataDirectiveMode(Enum):
    """ Mode of handling metadata when copying a file """
    COPY = 401  #: copy metadata from the source file
    REPLACE = 402  #: ignore the source file metadata and set it to provided values


@ensure_b2sdk_doc_urls
class LifecycleRule(TypedDict):
    """
    Lifecycle Rule.

    External documentation: `B2 Cloud Storage Lifecycle Rules`_.

    .. _B2 Cloud Storage Lifecycle Rules: https://www.backblaze.com/docs/cloud-storage-lifecycle-rules
    """
    fileNamePrefix: str
    daysFromHidingToDeleting: NotRequired[PositiveInt | None]
    daysFromUploadingToHiding: NotRequired[PositiveInt | None]
    daysFromStartingToCancelingUnfinishedLargeFiles: NotRequired[PositiveInt | None]


class NameValueDict(TypedDict):
    name: str
    value: str


class NotificationTargetConfiguration(TypedDict):
    """
    Notification Target Configuration.

    `hmacSha256SigningSecret`, if present, has to be a string of 32 alphanumeric characters.
    """
    # TODO: add URL to the documentation

    targetType: Literal['webhook']
    url: str
    customHeaders: NotRequired[list[NameValueDict] | None]
    hmacSha256SigningSecret: NotRequired[str | None]


EVENT_TYPE = Literal[
    'b2:ObjectCreated:*', 'b2:ObjectCreated:Upload', 'b2:ObjectCreated:MultipartUpload',
    'b2:ObjectCreated:Copy', 'b2:ObjectCreated:Replica', 'b2:ObjectCreated:MultipartReplica',
    'b2:ObjectDeleted:*', 'b2:ObjectDeleted:Delete', 'b2:ObjectDeleted:LifecycleRule',
    'b2:HideMarkerCreated:*', 'b2:HideMarkerCreated:Hide', 'b2:HideMarkerCreated:LifecycleRule',]


class _NotificationRule(TypedDict):
    """
    Notification Rule.
    """
    eventTypes: list[EVENT_TYPE]
    isEnabled: bool
    name: str
    objectNamePrefix: str
    targetConfiguration: NotificationTargetConfiguration
    suspensionReason: NotRequired[str]


class NotificationRule(_NotificationRule):
    """
    Notification Rule.

    When creating or modifying a notification rule, `isSuspended` and `suspensionReason` are ignored.
    """
    isSuspended: NotRequired[bool]


class NotificationRuleResponse(_NotificationRule):
    isSuspended: bool


def _bucket_notification_rule_feature_preview_warning(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            "Event Notifications feature is in \"Private Preview\" state and may change without notice. "
            "See https://www.backblaze.com/blog/announcing-event-notifications/ for details.",
            FeaturePreviewWarning,
            stacklevel=2,
        )
        return func(*args, **kwargs)

    return wrapper


@_bucket_notification_rule_feature_preview_warning
def notification_rule_response_to_request(rule: NotificationRuleResponse) -> NotificationRule:
    """
    Convert NotificationRuleResponse to NotificationRule.
    """
    rule = rule.copy()
    for key in ('isSuspended', 'suspensionReason'):
        rule.pop(key, None)
    return rule


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
        destination_server_side_encryption: EncryptionSetting | None = None,
        source_server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
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
        destination_server_side_encryption: EncryptionSetting | None = None,
        source_server_side_encryption: EncryptionSetting | None = None,
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
        lifecycle_rules: list[LifecycleRule] | None = None,
        default_server_side_encryption: EncryptionSetting | None = None,
        is_file_lock_enabled: bool | None = None,
        replication: ReplicationConfiguration | None = None,
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
        account_auth_token_or_none: str | None,
        url: str,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
    ):
        pass

    @abstractmethod
    def delete_key(self, api_url, account_auth_token, application_key_id):
        pass

    @abstractmethod
    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        pass

    @abstractmethod
    def delete_file_version(
        self, api_url, account_auth_token, file_id, file_name, bypass_governance: bool = False
    ):
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
                            file_id: str) -> dict[str, Any]:
        pass

    @abstractmethod
    def get_file_info_by_name(
        self, download_url: str, account_auth_token: str, bucket_name: str, file_name: str
    ) -> dict[str, Any]:
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
        server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
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
        lifecycle_rules: list[LifecycleRule] | None = None,
        if_revision_is=None,
        default_server_side_encryption: EncryptionSetting | None = None,
        default_retention: BucketRetentionSetting | None = None,
        replication: ReplicationConfiguration | None = None,
        is_file_lock_enabled: bool | None = None,
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

    @classmethod
    def get_upload_file_headers(
        cls,
        upload_auth_token: str,
        file_name: str,
        content_length: int,
        content_type: str,
        content_sha1: str,
        file_info: dict,
        server_side_encryption: EncryptionSetting | None,
        file_retention: FileRetentionSetting | None,
        legal_hold: LegalHold | None,
        custom_upload_timestamp: int | None = None,
    ) -> dict:
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-File-Name': b2_url_encode(file_name),
            'Content-Type': content_type,
            'X-Bz-Content-Sha1': content_sha1,
        }
        for k, v in file_info.items():
            headers[FILE_INFO_HEADER_PREFIX + k] = b2_url_encode(v)
        if server_side_encryption is not None:
            assert server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            server_side_encryption.add_to_upload_headers(headers)

        if legal_hold is not None:
            legal_hold.add_to_upload_headers(headers)

        if file_retention is not None:
            file_retention.add_to_to_upload_headers(headers)

        if custom_upload_timestamp is not None:
            headers['X-Bz-Custom-Upload-Timestamp'] = str(custom_upload_timestamp)

        return headers

    @abstractmethod
    def upload_file(
        self,
        upload_url,
        upload_auth_token,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
        data_stream,
        server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
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
        server_side_encryption: EncryptionSetting | None = None,
    ):
        pass

    def get_download_url_by_id(self, download_url, file_id):
        return f'{download_url}/b2api/{API_VERSION}/b2_download_file_by_id?fileId={file_id}'

    def get_download_url_by_name(self, download_url, bucket_name, file_name):
        return download_url + '/file/' + bucket_name + '/' + b2_url_encode(file_name)

    @abstractmethod
    def set_bucket_notification_rules(
        self, api_url: str, account_auth_token: str, bucket_id: str,
        rules: Iterable[NotificationRule]
    ) -> list[NotificationRuleResponse]:
        pass

    @abstractmethod
    def get_bucket_notification_rules(self, api_url: str, account_auth_token: str,
                                      bucket_id: str) -> list[NotificationRuleResponse]:
        pass


class B2RawHTTPApi(AbstractRawApi):
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

    def _post_json(self, base_url: str, endpoint: str, auth: str, **params) -> JSON:
        """
        A helper method for calling an API with the given auth and params.

        :param base_url: something like "https://api001.backblazeb2.com/"
        :param auth: passed in Authorization header
        :param endpoint: example: "b2_create_bucket"
        :param args: the rest of the parameters are passed to b2
        :return: the decoded JSON response
        """
        url = f'{base_url}/b2api/{API_VERSION}/{endpoint}'
        headers = {'Authorization': auth}
        return self.b2_http.post_json_return_json(url, headers, params)

    def _get_json(self, base_url: str, endpoint: str, auth: str, **params) -> JSON:
        url = f'{base_url}/b2api/{API_VERSION}/{endpoint}'
        headers = {'Authorization': auth}
        return self.b2_http.request_content_return_json('GET', url, headers, params=params)

    def authorize_account(self, realm_url, application_key_id, application_key):
        auth = f"Basic {base64.b64encode(f'{application_key_id}:{application_key}'.encode()).decode()}"
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
        lifecycle_rules: list[LifecycleRule] | None = None,
        default_server_side_encryption: EncryptionSetting | None = None,
        is_file_lock_enabled: bool | None = None,
        replication: ReplicationConfiguration | None = None,
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
        if replication is not None:
            kwargs['replicationConfiguration'] = replication.serialize_to_json_for_request()
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

    def delete_file_version(
        self, api_url, account_auth_token, file_id, file_name, bypass_governance: bool = False
    ):
        return self._post_json(
            api_url,
            'b2_delete_file_version',
            account_auth_token,
            fileId=file_id,
            fileName=file_name,
            bypassGovernance=bypass_governance,
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
        account_auth_token_or_none: str | None,
        url: str,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
    ):
        """
        Issue a streaming request for download of a file, potentially authorized.

        :param account_auth_token_or_none: an optional account auth token to pass in
        :param url: the full URL to download from
        :param range_: two-element tuple for http Range header
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings for downloading
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
                            file_id: str) -> dict[str, Any]:
        return self._post_json(api_url, 'b2_get_file_info', account_auth_token, fileId=file_id)

    def get_file_info_by_name(
        self, download_url: str, account_auth_token: str, bucket_name: str, file_name: str
    ) -> dict[str, Any]:
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
        server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        kwargs = {}
        if server_side_encryption is not None:
            assert server_side_encryption.mode in (
                EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C
            )
            kwargs['serverSideEncryption'] = server_side_encryption.serialize_to_json_for_request()

            if server_side_encryption.mode == EncryptionMode.SSE_C:
                file_info = server_side_encryption.add_key_id_to_file_info(file_info)

        if legal_hold is not None:
            kwargs['legalHold'] = legal_hold.to_server()

        if file_retention is not None:
            kwargs['fileRetention'] = file_retention.serialize_to_json_for_request()

        if custom_upload_timestamp is not None:
            kwargs['custom_upload_timestamp'] = custom_upload_timestamp

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
        lifecycle_rules: list[LifecycleRule] | None = None,
        if_revision_is=None,
        default_server_side_encryption: EncryptionSetting | None = None,
        default_retention: BucketRetentionSetting | None = None,
        replication: ReplicationConfiguration | None = None,
        is_file_lock_enabled: bool | None = None,
    ):
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
        if replication is not None:
            kwargs['replicationConfiguration'] = replication.serialize_to_json_for_request()
        if is_file_lock_enabled is not None:
            kwargs['fileLockEnabled'] = is_file_lock_enabled

        assert kwargs

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
            message = "Filename \"{}\" contains code {} (hex {:02x}), less than 32.".format(
                unprintable_to_hex(filename), lowest_unicode_value, lowest_unicode_value
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
        file_info: dict,
        data_stream,
        server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        """
        Upload one, small file to b2.

        :param upload_url: the upload_url from b2_authorize_account
        :param upload_auth_token: the auth token from b2_authorize_account
        :param file_name: the name of the B2 file
        :param content_length: number of bytes in the file
        :param content_type: MIME type
        :param content_sha1: hex SHA1 of the contents of the file
        :param file_info: extra file info to upload
        :param data_stream: a file like object from which the contents of the file can be read
        :param server_side_encryption: encryption setting for the file
        :param file_retention: retention setting for the file
        :param legal_hold: legal hold setting for the file
        :param custom_upload_timestamp: custom upload timestamp for the file
        :return:
        """
        # Raise UnusableFileName if the file_name doesn't meet the rules.
        self.check_b2_filename(file_name)
        headers = self.get_upload_file_headers(
            upload_auth_token=upload_auth_token,
            file_name=file_name,
            content_length=content_length,
            content_type=content_type,
            content_sha1=content_sha1,
            file_info=file_info,
            server_side_encryption=server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            custom_upload_timestamp=custom_upload_timestamp,
        )
        return self.b2_http.post_content_return_json(upload_url, headers, data_stream)

    def upload_part(
        self,
        upload_url,
        upload_auth_token,
        part_number,
        content_length,
        content_sha1,
        data_stream,
        server_side_encryption: EncryptionSetting | None = None,
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
        destination_server_side_encryption: EncryptionSetting | None = None,
        source_server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
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
        destination_server_side_encryption: EncryptionSetting | None = None,
        source_server_side_encryption: EncryptionSetting | None = None,
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

    @_bucket_notification_rule_feature_preview_warning
    def set_bucket_notification_rules(
        self, api_url: str, account_auth_token: str, bucket_id: str, rules: list[NotificationRule]
    ) -> list[NotificationRuleResponse]:
        return self._post_json(
            api_url,
            'b2_set_bucket_notification_rules',
            account_auth_token,
            **{
                'bucketId': bucket_id,
                'eventNotificationRules': rules,
            },
        )["eventNotificationRules"]

    @_bucket_notification_rule_feature_preview_warning
    def get_bucket_notification_rules(self, api_url: str, account_auth_token: str,
                                      bucket_id: str) -> list[NotificationRuleResponse]:
        return self._get_json(
            api_url,
            'b2_get_bucket_notification_rules',
            account_auth_token,
            **{
                'bucketId': bucket_id,
            },
        )["eventNotificationRules"]


def _add_range_header(headers, range_):
    if range_ is not None:
        assert len(range_) == 2, range_
        assert (range_[0] + 0) <= (range_[1] + 0), range_  # not strings
        assert range_[0] >= 0, range_
        headers['Range'] = "bytes=%d-%d" % range_
