######################################################################
#
# File: b2sdk/v2/__init__.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk.v3 import *  # noqa
from b2sdk.v3 import parse_folder as parse_sync_folder
from b2sdk.v3 import AbstractPath as AbstractSyncPath
from b2sdk.v3 import LocalPath as LocalSyncPath
from b2sdk._internal.utils.escape import (
    unprintable_to_hex,
    escape_control_chars,
    substitute_control_chars,
)

from .account_info import AbstractAccountInfo
from .account_info import InMemoryAccountInfo
from .account_info import SqliteAccountInfo
from .account_info import StubAccountInfo
from .account_info import UrlPoolAccountInfo
from .api import B2Api, Services
from .b2http import B2Http
from .api_config import B2HttpApiConfig, DEFAULT_HTTP_API_CONFIG
from .bucket import Bucket, BucketFactory
from .session import B2Session
from .sync import B2SyncPath
from .transfer import DownloadManager, UploadManager

# replication

from .replication.setup import ReplicationSetupHelper

# data classes

from .application_key import ApplicationKey
from .application_key import BaseApplicationKey
from .application_key import FullApplicationKey

# utils

from .version_utils import rename_argument, rename_function
from .utils import TempDir

# raw_simulator

from .raw_simulator import BucketSimulator
from .raw_simulator import RawSimulator
from .raw_simulator import KeySimulator

# raw_api

from .raw_api import AbstractRawApi
from .raw_api import B2RawHTTPApi

# file_version

from .file_version import FileVersion
from .file_version import FileVersionFactory

# large_file

from .large_file import LargeFileServices
from .large_file import UnfinishedLargeFile
