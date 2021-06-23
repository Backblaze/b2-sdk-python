######################################################################
#
# File: b2sdk/_v2/__init__.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# TODO: v2 is still in progress that why the path is prefixed with "_"

# this file maps the external interface into internal interface
# it will come handy if we ever need to move something

# core

from b2sdk.api import B2Api
from b2sdk.bucket import Bucket
from b2sdk.bucket import BucketFactory
from b2sdk.raw_api import ALL_CAPABILITIES, REALM_URLS

# encryption

from b2sdk.encryption.setting import EncryptionSetting
from b2sdk.encryption.setting import EncryptionSettingFactory
from b2sdk.encryption.setting import EncryptionKey
from b2sdk.encryption.setting import SSE_NONE, SSE_B2_AES, UNKNOWN_KEY_ID
from b2sdk.encryption.types import EncryptionAlgorithm
from b2sdk.encryption.types import EncryptionMode
from b2sdk.http_constants import SSE_C_KEY_ID_FILE_INFO_KEY_NAME

# account info

from b2sdk.account_info.abstract import AbstractAccountInfo
from b2sdk.account_info.in_memory import InMemoryAccountInfo
from b2sdk.account_info.sqlite_account_info import SqliteAccountInfo
from b2sdk.account_info.sqlite_account_info import B2_ACCOUNT_INFO_ENV_VAR, B2_ACCOUNT_INFO_DEFAULT_FILE, XDG_CONFIG_HOME_ENV_VAR
from b2sdk.account_info.stub import StubAccountInfo
from b2sdk.account_info.upload_url_pool import UploadUrlPool
from b2sdk.account_info.upload_url_pool import UrlPoolAccountInfo

# version & version utils

from b2sdk.version import VERSION, USER_AGENT
from b2sdk.version_utils import rename_argument, rename_function

# utils

from b2sdk.utils import (
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
    TempDir,
)

from b2sdk.utils import trace_call

# data classes

from b2sdk.application_key import ApplicationKey
from b2sdk.application_key import BaseApplicationKey
from b2sdk.application_key import FullApplicationKey
from b2sdk.file_version import DownloadVersion
from b2sdk.file_version import DownloadVersionFactory
from b2sdk.file_version import FileIdAndName
from b2sdk.file_version import FileVersion
from b2sdk.file_version import FileVersionFactory
from b2sdk.large_file.part import Part
from b2sdk.large_file.unfinished_large_file import UnfinishedLargeFile
from b2sdk.utils.range_ import Range

# file lock

from b2sdk.file_lock import BucketRetentionSetting
from b2sdk.file_lock import FileLockConfiguration
from b2sdk.file_lock import FileRetentionSetting
from b2sdk.file_lock import LegalHold
from b2sdk.file_lock import NO_RETENTION_BUCKET_SETTING
from b2sdk.file_lock import NO_RETENTION_FILE_SETTING
from b2sdk.file_lock import RetentionMode
from b2sdk.file_lock import RetentionPeriod
from b2sdk.file_lock import UNKNOWN_BUCKET_RETENTION
from b2sdk.file_lock import UNKNOWN_FILE_LOCK_CONFIGURATION
from b2sdk.file_lock import UNKNOWN_FILE_RETENTION_SETTING

# progress reporting

from b2sdk.progress import AbstractProgressListener
from b2sdk.progress import DoNothingProgressListener
from b2sdk.progress import ProgressListenerForTest
from b2sdk.progress import SimpleProgressListener
from b2sdk.progress import TqdmProgressListener
from b2sdk.progress import make_progress_listener

# raw_simulator

from b2sdk.raw_simulator import BucketSimulator
from b2sdk.raw_simulator import FakeResponse
from b2sdk.raw_simulator import FileSimulator
from b2sdk.raw_simulator import KeySimulator
from b2sdk.raw_simulator import PartSimulator
from b2sdk.raw_simulator import RawSimulator

# raw_api

from b2sdk.raw_api import AbstractRawApi
from b2sdk.raw_api import B2RawHTTPApi
from b2sdk.raw_api import MetadataDirectiveMode

# stream

from b2sdk.stream.progress import AbstractStreamWithProgress
from b2sdk.stream import RangeOfInputStream
from b2sdk.stream import ReadingStreamWithProgress
from b2sdk.stream import StreamWithHash
from b2sdk.stream import WritingStreamWithProgress

# source / destination

from b2sdk.transfer.inbound.downloaded_file import DownloadedFile

from b2sdk.transfer.outbound.outbound_source import OutboundTransferSource
from b2sdk.transfer.outbound.copy_source import CopySource
from b2sdk.transfer.outbound.upload_source import AbstractUploadSource
from b2sdk.transfer.outbound.upload_source import UploadSourceBytes
from b2sdk.transfer.outbound.upload_source import UploadSourceLocalFile
from b2sdk.transfer.outbound.upload_source import UploadSourceLocalFileRange
from b2sdk.transfer.outbound.upload_source import UploadSourceStream
from b2sdk.transfer.outbound.upload_source import UploadSourceStreamRange

from b2sdk.transfer.emerge.write_intent import WriteIntent

# trasfer

from b2sdk.transfer.inbound.downloader.abstract import AbstractDownloader
from b2sdk.transfer.outbound.large_file_upload_state import LargeFileUploadState
from b2sdk.transfer.inbound.downloader.parallel import AbstractDownloaderThread
from b2sdk.transfer.inbound.downloader.parallel import FirstPartDownloaderThread
from b2sdk.transfer.inbound.downloader.parallel import NonHashingDownloaderThread
from b2sdk.transfer.inbound.downloader.parallel import ParallelDownloader
from b2sdk.transfer.inbound.downloader.parallel import PartToDownload
from b2sdk.transfer.inbound.downloader.parallel import WriterThread
from b2sdk.transfer.outbound.progress_reporter import PartProgressReporter
from b2sdk.transfer.inbound.downloader.simple import SimpleDownloader

# sync

from b2sdk.sync.action import AbstractAction
from b2sdk.sync.action import B2CopyAction
from b2sdk.sync.action import B2DeleteAction
from b2sdk.sync.action import B2DownloadAction
from b2sdk.sync.action import B2HideAction
from b2sdk.sync.action import B2UploadAction
from b2sdk.sync.action import LocalDeleteAction
from b2sdk.sync.exception import EnvironmentEncodingError
from b2sdk.sync.exception import IncompleteSync
from b2sdk.sync.exception import InvalidArgument
from b2sdk.sync.folder import AbstractFolder
from b2sdk.sync.folder import B2Folder
from b2sdk.sync.folder import LocalFolder
from b2sdk.sync.folder_parser import parse_sync_folder
from b2sdk.sync.path import AbstractSyncPath, B2SyncPath, LocalSyncPath
from b2sdk.sync.policy import AbstractFileSyncPolicy
from b2sdk.sync.policy import CompareVersionMode
from b2sdk.sync.policy import NewerFileSyncMode
from b2sdk.sync.policy import DownAndDeletePolicy
from b2sdk.sync.policy import DownAndKeepDaysPolicy
from b2sdk.sync.policy import DownPolicy
from b2sdk.sync.policy import CopyPolicy
from b2sdk.sync.policy import CopyAndDeletePolicy
from b2sdk.sync.policy import CopyAndKeepDaysPolicy
from b2sdk.sync.policy import UpAndDeletePolicy
from b2sdk.sync.policy import UpAndKeepDaysPolicy
from b2sdk.sync.policy import UpPolicy
from b2sdk.sync.policy import make_b2_keep_days_actions
from b2sdk.sync.policy_manager import SyncPolicyManager
from b2sdk.sync.policy_manager import POLICY_MANAGER
from b2sdk.sync.report import SyncFileReporter
from b2sdk.sync.report import SyncReport
from b2sdk.sync.scan_policies import DEFAULT_SCAN_MANAGER
from b2sdk.sync.scan_policies import IntegerRange
from b2sdk.sync.scan_policies import RegexSet
from b2sdk.sync.scan_policies import ScanPoliciesManager
from b2sdk.sync.scan_policies import convert_dir_regex_to_dir_prefix_regex
from b2sdk.sync.sync import KeepOrDeleteMode
from b2sdk.sync.sync import Synchronizer
from b2sdk.sync.sync import zip_folders
from b2sdk.sync.encryption_provider import AbstractSyncEncryptionSettingsProvider
from b2sdk.sync.encryption_provider import BasicSyncEncryptionSettingsProvider
from b2sdk.sync.encryption_provider import ServerDefaultSyncEncryptionSettingsProvider
from b2sdk.sync.encryption_provider import SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER

# other

from b2sdk.b2http import B2Http
from b2sdk.api_config import B2HttpApiConfig
from b2sdk.api_config import DEFAULT_HTTP_API_CONFIG
from b2sdk.b2http import ClockSkewHook
from b2sdk.b2http import HttpCallback
from b2sdk.b2http import ResponseContextManager
from b2sdk.b2http import _translate_and_retry as translate_and_retry  # for some reason importing a _private thing didn't work in tests
from b2sdk.b2http import _translate_errors as translate_errors
from b2sdk.bounded_queue_executor import BoundedQueueExecutor
from b2sdk.cache import AbstractCache
from b2sdk.cache import AuthInfoCache
from b2sdk.cache import DummyCache
from b2sdk.cache import InMemoryCache
from b2sdk.http_constants import SRC_LAST_MODIFIED_MILLIS
from b2sdk.session import B2Session
