######################################################################
#
# File: b2sdk/v1/sync/encryption_provider.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import inspect
from abc import abstractmethod

from b2sdk import v2
from ..bucket import Bucket
from ..file_version import FileVersionInfo


#  wrapper to translate new argument names to old ones
class SyncEncryptionSettingsProviderWrapper(v2.AbstractSyncEncryptionSettingsProvider):
    def __init__(self, provider):
        self.provider = provider

    def __repr__(self):
        return f"{self.__class__.__name__}({self.provider})"

    def get_setting_for_upload(
        self,
        bucket: Bucket,
        b2_file_name: str,
        file_info: dict | None,
        length: int,
    ) -> v2.EncryptionSetting | None:
        return self.provider.get_setting_for_upload(
            bucket=bucket,
            b2_file_name=b2_file_name,
            file_info=file_info,
            length=length,
        )

    def get_source_setting_for_copy(
        self,
        bucket: Bucket,
        source_file_version: v2.FileVersion,
    ) -> v2.EncryptionSetting | None:
        return self.provider.get_source_setting_for_copy(
            bucket=bucket, source_file_version_info=source_file_version
        )

    def get_destination_setting_for_copy(
        self,
        bucket: Bucket,
        dest_b2_file_name: str,
        source_file_version: v2.FileVersion,
        target_file_info: dict | None = None,
    ) -> v2.EncryptionSetting | None:
        return self.provider.get_destination_setting_for_copy(
            bucket=bucket,
            dest_b2_file_name=dest_b2_file_name,
            source_file_version_info=source_file_version,
            target_file_info=target_file_info,
        )

    def get_setting_for_download(
        self,
        bucket: Bucket,
        file_version: v2.FileVersion,
    ) -> v2.EncryptionSetting | None:
        return self.provider.get_setting_for_download(
            bucket=bucket,
            file_version_info=file_version,
        )


def wrap_if_necessary(provider):
    if 'file_version' in inspect.getfullargspec(provider.get_setting_for_download).args:
        return provider
    return SyncEncryptionSettingsProviderWrapper(provider)


#  Old signatures
class AbstractSyncEncryptionSettingsProvider(v2.AbstractSyncEncryptionSettingsProvider):
    @abstractmethod
    def get_setting_for_upload(
        self,
        bucket: Bucket,
        b2_file_name: str,
        file_info: dict | None,
        length: int,
    ) -> v2.EncryptionSetting | None:
        """
        Return an EncryptionSetting for uploading an object or None if server should decide.
        """

    @abstractmethod
    def get_source_setting_for_copy(
        self,
        bucket: Bucket,
        source_file_version_info: FileVersionInfo,
    ) -> v2.EncryptionSetting | None:
        """
        Return an EncryptionSetting for a source of copying an object or None if not required
        """

    @abstractmethod
    def get_destination_setting_for_copy(
        self,
        bucket: Bucket,
        dest_b2_file_name: str,
        source_file_version_info: FileVersionInfo,
        target_file_info: dict | None = None,
    ) -> v2.EncryptionSetting | None:
        """
        Return an EncryptionSetting for a destination for copying an object or None if server should decide
        """

    @abstractmethod
    def get_setting_for_download(
        self,
        bucket: Bucket,
        file_version_info: FileVersionInfo,
    ) -> v2.EncryptionSetting | None:
        """
        Return an EncryptionSetting for downloading an object from, or None if not required
        """
