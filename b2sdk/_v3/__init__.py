######################################################################
#
# File: b2sdk/_v3/__init__.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

# Set default logging handler to avoid "No handler found" warnings.
import logging as _logging

_logging.getLogger("b2sdk").addHandler(_logging.NullHandler())


class UrllibWarningFilter:
    def filter(self, record):
        return record.msg != "Connection pool is full, discarding connection: %s"


_logging.getLogger('urllib3.connectionpool').addFilter(UrllibWarningFilter())

# this file maps the external interface into internal interface
# it will come handy if we ever need to move something

# core

from b2sdk._internal.api import B2Api
from b2sdk._internal.api import Services
from b2sdk._internal.bucket import Bucket
from b2sdk._internal.bucket import BucketFactory
from b2sdk._internal.raw_api import ALL_CAPABILITIES, REALM_URLS, EVENT_TYPE

# encryption

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.encryption.setting import EncryptionSettingFactory
from b2sdk._internal.encryption.setting import EncryptionKey
from b2sdk._internal.encryption.setting import SSE_NONE, SSE_B2_AES, UNKNOWN_KEY_ID
from b2sdk._internal.encryption.types import EncryptionAlgorithm
from b2sdk._internal.encryption.types import EncryptionMode
from b2sdk._internal.http_constants import SSE_C_KEY_ID_FILE_INFO_KEY_NAME

# account info

from b2sdk._internal.account_info.abstract import AbstractAccountInfo
from b2sdk._internal.account_info.in_memory import InMemoryAccountInfo
from b2sdk._internal.account_info.sqlite_account_info import SqliteAccountInfo
from b2sdk._internal.account_info.sqlite_account_info import B2_ACCOUNT_INFO_ENV_VAR
from b2sdk._internal.account_info.sqlite_account_info import B2_ACCOUNT_INFO_DEFAULT_FILE
from b2sdk._internal.account_info.sqlite_account_info import B2_ACCOUNT_INFO_PROFILE_FILE
from b2sdk._internal.account_info.sqlite_account_info import XDG_CONFIG_HOME_ENV_VAR
from b2sdk._internal.account_info.stub import StubAccountInfo
from b2sdk._internal.account_info.upload_url_pool import UploadUrlPool
from b2sdk._internal.account_info.upload_url_pool import UrlPoolAccountInfo

# version & version utils

from b2sdk.version import VERSION, USER_AGENT
from b2sdk._internal.version_utils import rename_argument, rename_function, FeaturePreviewWarning

# utils

from b2sdk._internal.utils import (
    b2_url_encode,
    b2_url_decode,
    choose_part_ranges,
    current_time_millis,
    fix_windows_path_limit,
    format_and_scale_fraction,
    format_and_scale_number,
    hex_sha1_of_stream,
    hex_sha1_of_bytes,
    hex_sha1_of_file,
    IncrementalHexDigester,
)

from b2sdk._internal.utils.filesystem import (
    points_to_fifo,
    points_to_stdout,
    STDOUT_FILEPATH,
)
from b2sdk._internal.utils import trace_call
from b2sdk._internal.utils.docs import get_b2sdk_doc_urls

# data classes

from b2sdk._internal.application_key import ApplicationKey
from b2sdk._internal.application_key import BaseApplicationKey
from b2sdk._internal.application_key import FullApplicationKey
from b2sdk._internal.file_version import DownloadVersion
from b2sdk._internal.file_version import DownloadVersionFactory
from b2sdk._internal.file_version import FileIdAndName
from b2sdk._internal.file_version import FileVersion
from b2sdk._internal.file_version import FileVersionFactory
from b2sdk._internal.large_file.part import Part
from b2sdk._internal.large_file.unfinished_large_file import UnfinishedLargeFile
from b2sdk._internal.large_file.services import LargeFileServices
from b2sdk._internal.utils.range_ import Range, EMPTY_RANGE

# file lock

from b2sdk._internal.file_lock import BucketRetentionSetting
from b2sdk._internal.file_lock import FileLockConfiguration
from b2sdk._internal.file_lock import FileRetentionSetting
from b2sdk._internal.file_lock import LegalHold
from b2sdk._internal.file_lock import NO_RETENTION_BUCKET_SETTING
from b2sdk._internal.file_lock import NO_RETENTION_FILE_SETTING
from b2sdk._internal.file_lock import RetentionMode
from b2sdk._internal.file_lock import RetentionPeriod
from b2sdk._internal.file_lock import UNKNOWN_BUCKET_RETENTION
from b2sdk._internal.file_lock import UNKNOWN_FILE_LOCK_CONFIGURATION
from b2sdk._internal.file_lock import UNKNOWN_FILE_RETENTION_SETTING

# progress reporting

from b2sdk._internal.progress import AbstractProgressListener
from b2sdk._internal.progress import DoNothingProgressListener
from b2sdk._internal.progress import ProgressListenerForTest
from b2sdk._internal.progress import SimpleProgressListener
from b2sdk._internal.progress import TqdmProgressListener
from b2sdk._internal.progress import make_progress_listener

# raw_simulator

from b2sdk._internal.raw_simulator import BucketSimulator
from b2sdk._internal.raw_simulator import FakeResponse
from b2sdk._internal.raw_simulator import FileSimulator
from b2sdk._internal.raw_simulator import KeySimulator
from b2sdk._internal.raw_simulator import PartSimulator
from b2sdk._internal.raw_simulator import RawSimulator

# raw_api

from b2sdk._internal.raw_api import AbstractRawApi
from b2sdk._internal.raw_api import B2RawHTTPApi
from b2sdk._internal.raw_api import MetadataDirectiveMode
from b2sdk._internal.raw_api import LifecycleRule
from b2sdk._internal.raw_api import NotificationRule, NotificationRuleResponse, notification_rule_response_to_request

# stream

from b2sdk._internal.stream.chained import StreamOpener
from b2sdk._internal.stream.progress import AbstractStreamWithProgress
from b2sdk._internal.stream import RangeOfInputStream
from b2sdk._internal.stream import ReadingStreamWithProgress
from b2sdk._internal.stream import StreamWithHash
from b2sdk._internal.stream import WritingStreamWithProgress

# source / destination

from b2sdk._internal.transfer.inbound.downloaded_file import DownloadedFile
from b2sdk._internal.transfer.inbound.downloaded_file import MtimeUpdatedFile
from b2sdk._internal.transfer.inbound.download_manager import DownloadManager

from b2sdk._internal.transfer.outbound.outbound_source import OutboundTransferSource
from b2sdk._internal.transfer.outbound.copy_source import CopySource
from b2sdk._internal.transfer.outbound.upload_source import AbstractUploadSource
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceBytes
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceLocalFile
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceLocalFileRange
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceStream
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceStreamRange
from b2sdk._internal.transfer.outbound.upload_manager import UploadManager

from b2sdk._internal.transfer.emerge.planner.upload_subpart import CachedBytesStreamOpener
from b2sdk._internal.transfer.emerge.write_intent import WriteIntent

# transfer

from b2sdk._internal.transfer.inbound.downloader.abstract import AbstractDownloader
from b2sdk._internal.transfer.outbound.large_file_upload_state import LargeFileUploadState
from b2sdk._internal.transfer.inbound.downloader.parallel import ParallelDownloader
from b2sdk._internal.transfer.inbound.downloader.parallel import PartToDownload
from b2sdk._internal.transfer.inbound.downloader.parallel import WriterThread
from b2sdk._internal.transfer.outbound.progress_reporter import PartProgressReporter
from b2sdk._internal.transfer.inbound.downloader.simple import SimpleDownloader

# sync

from b2sdk._internal.sync.action import AbstractAction
from b2sdk._internal.sync.action import B2CopyAction
from b2sdk._internal.sync.action import B2DeleteAction
from b2sdk._internal.sync.action import B2DownloadAction
from b2sdk._internal.sync.action import B2HideAction
from b2sdk._internal.sync.action import B2UploadAction
from b2sdk._internal.sync.action import LocalDeleteAction
from b2sdk._internal.sync.exception import IncompleteSync
from b2sdk._internal.sync.policy import AbstractFileSyncPolicy
from b2sdk._internal.sync.policy import CompareVersionMode
from b2sdk._internal.sync.policy import NewerFileSyncMode
from b2sdk._internal.sync.policy import DownAndDeletePolicy
from b2sdk._internal.sync.policy import DownAndKeepDaysPolicy
from b2sdk._internal.sync.policy import DownPolicy
from b2sdk._internal.sync.policy import CopyPolicy
from b2sdk._internal.sync.policy import CopyAndDeletePolicy
from b2sdk._internal.sync.policy import CopyAndKeepDaysPolicy
from b2sdk._internal.sync.policy import UpAndDeletePolicy
from b2sdk._internal.sync.policy import UpAndKeepDaysPolicy
from b2sdk._internal.sync.policy import UpPolicy
from b2sdk._internal.sync.policy import make_b2_keep_days_actions
from b2sdk._internal.sync.policy_manager import SyncPolicyManager
from b2sdk._internal.sync.policy_manager import POLICY_MANAGER
from b2sdk._internal.sync.report import SyncFileReporter
from b2sdk._internal.sync.report import SyncReport
from b2sdk._internal.sync.sync import KeepOrDeleteMode
from b2sdk._internal.sync.sync import Synchronizer
from b2sdk._internal.sync.sync import UploadMode
from b2sdk._internal.sync.encryption_provider import AbstractSyncEncryptionSettingsProvider
from b2sdk._internal.sync.encryption_provider import BasicSyncEncryptionSettingsProvider
from b2sdk._internal.sync.encryption_provider import ServerDefaultSyncEncryptionSettingsProvider
from b2sdk._internal.sync.encryption_provider import SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER

# scan

from b2sdk._internal.scan.exception import EnvironmentEncodingError
from b2sdk._internal.scan.exception import InvalidArgument
from b2sdk._internal.scan.folder import AbstractFolder
from b2sdk._internal.scan.folder import B2Folder
from b2sdk._internal.scan.folder import LocalFolder
from b2sdk._internal.scan.folder_parser import parse_folder
from b2sdk._internal.scan.path import AbstractPath, B2Path, LocalPath
from b2sdk._internal.scan.policies import convert_dir_regex_to_dir_prefix_regex
from b2sdk._internal.scan.policies import DEFAULT_SCAN_MANAGER
from b2sdk._internal.scan.policies import IntegerRange
from b2sdk._internal.scan.policies import RegexSet
from b2sdk._internal.scan.policies import ScanPoliciesManager
from b2sdk._internal.scan.report import ProgressReport
from b2sdk._internal.scan.scan import zip_folders
from b2sdk._internal.scan.scan import AbstractScanResult
from b2sdk._internal.scan.scan import AbstractScanReport
from b2sdk._internal.scan.scan import CountAndSampleScanReport

# replication

from b2sdk._internal.replication.setting import ReplicationConfigurationFactory
from b2sdk._internal.replication.setting import ReplicationConfiguration
from b2sdk._internal.replication.setting import ReplicationRule
from b2sdk._internal.replication.types import ReplicationStatus
from b2sdk._internal.replication.setup import ReplicationSetupHelper
from b2sdk._internal.replication.monitoring import ReplicationScanResult
from b2sdk._internal.replication.monitoring import ReplicationReport
from b2sdk._internal.replication.monitoring import ReplicationMonitor

# other

from b2sdk._internal.included_sources import get_included_sources
from b2sdk._internal.b2http import B2Http
from b2sdk._internal.api_config import B2HttpApiConfig
from b2sdk._internal.api_config import DEFAULT_HTTP_API_CONFIG
from b2sdk._internal.b2http import ClockSkewHook
from b2sdk._internal.b2http import HttpCallback
from b2sdk._internal.b2http import ResponseContextManager
from b2sdk._internal.bounded_queue_executor import BoundedQueueExecutor
from b2sdk._internal.cache import AbstractCache
from b2sdk._internal.cache import AuthInfoCache
from b2sdk._internal.cache import DummyCache
from b2sdk._internal.cache import InMemoryCache
from b2sdk._internal.http_constants import (
    BUCKET_NAME_CHARS,
    BUCKET_NAME_CHARS_UNIQ,
    BUCKET_NAME_LENGTH_RANGE,
    DEFAULT_MAX_PART_SIZE,
    DEFAULT_MIN_PART_SIZE,
    DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE,
    LARGE_FILE_SHA1,
    LIST_FILE_NAMES_MAX_LIMIT,
    SRC_LAST_MODIFIED_MILLIS,
)
from b2sdk._internal.session import B2Session
from b2sdk._internal.utils.thread_pool import ThreadPoolMixin
from b2sdk._internal.utils.escape import unprintable_to_hex, escape_control_chars, substitute_control_chars

# filter
from b2sdk._internal.filter import FilterType, Filter

# typing
from b2sdk._internal.utils.typing import JSON
