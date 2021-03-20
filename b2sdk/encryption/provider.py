######################################################################
#
# File: b2sdk/encryption/provider.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional

from ..file_version import FileVersionInfo
from .setting import EncryptionSetting


class AbstractEncryptionSettingsProvider(metaclass=ABCMeta):
    """
    Object which provides an appropriate EncryptionSetting object
    for complex operations with multiple sources and destinations such
    as sync and create_file
    """

    @abstractmethod
    def get_setting_for_destination(self, bucket,
                                    file_version: FileVersionInfo) -> Optional[EncryptionSetting]:
        """
        returns an EncryptionSetting for uploading/copying an object from, or None if server should decide
        """

    @abstractmethod
    def get_setting_for_source(self, bucket,
                               file_version: FileVersionInfo) -> Optional[EncryptionSetting]:
        """
        returns an EncryptionSetting for downloading/copying an object from, or None if server should decide
        """


class ServerDefaultEncryptionSettingsProvider(AbstractEncryptionSettingsProvider):
    """
    Encryption settings provider which assumes setting-less (2021-03-18: no encryption or SSE-B2) reads
    and a bucket default for writes.
    """

    def get_setting_for_destination(self, bucket, file_version) -> None:
        return None

    def get_setting_for_source(self, bucket, file_version) -> None:
        return None


class BasicEncryptionSettingsProvider(AbstractEncryptionSettingsProvider):
    """
    Basic encryption setting provider that supports exactly one encryption setting per bucket
    """

    def __init__(self, bucket_settings: Dict[str, EncryptionSetting]):
        """
        bucket_settings is a mapping from bucket name to EncryptionSetting object
        """
        self.bucket_settings = bucket_settings

    def get_setting_for_destination(
        self,
        bucket,
        file_version: FileVersionInfo,
    ) -> Optional[EncryptionSetting]:
        return self.bucket_settings[bucket.name]

    def get_setting_for_source(
        self,
        bucket,
        file_version: FileVersionInfo,
    ) -> Optional[EncryptionSetting]:
        return self.bucket_settings[bucket.name]
