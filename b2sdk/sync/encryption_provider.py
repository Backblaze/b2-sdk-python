######################################################################
#
# File: b2sdk/sync/encryption_provider.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional

from ..encryption.setting import EncryptionSetting
from ..bucket import Bucket
from ..file_version import FileVersion


class AbstractSyncEncryptionSettingsProvider(metaclass=ABCMeta):
    """
    Object which provides an appropriate EncryptionSetting object
    for sync, i.e. complex operations with multiple sources and destinations
    """

    @abstractmethod
    def get_setting_for_upload(
        self,
        bucket: Bucket,
        b2_file_name: str,
        file_info: Optional[dict],
        length: int,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for uploading an object or None if server should decide.
        """

    @abstractmethod
    def get_source_setting_for_copy(
        self,
        bucket: Bucket,
        source_file_version: FileVersion,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for a source of copying an object or None if not required
        """

    @abstractmethod
    def get_destination_setting_for_copy(
        self,
        bucket: Bucket,
        dest_b2_file_name: str,
        source_file_version: FileVersion,
        target_file_info: Optional[dict] = None,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for a destination for copying an object or None if server should decide
        """

    @abstractmethod
    def get_setting_for_download(
        self,
        bucket: Bucket,
        file_version: FileVersion,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for downloading an object from, or None if not required
        """


class ServerDefaultSyncEncryptionSettingsProvider(AbstractSyncEncryptionSettingsProvider):
    """
    Encryption settings provider which assumes setting-less reads
    and a bucket default for writes.
    """

    def get_setting_for_upload(self, *args, **kwargs) -> None:
        return None

    def get_source_setting_for_copy(self, *args, **kwargs) -> None:
        return None

    def get_destination_setting_for_copy(self, *args, **kwargs) -> None:
        return None

    def get_setting_for_download(self, *args, **kwargs) -> None:
        return None


SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER = ServerDefaultSyncEncryptionSettingsProvider()


class BasicSyncEncryptionSettingsProvider(AbstractSyncEncryptionSettingsProvider):
    """
    Basic encryption setting provider that supports exactly one encryption setting per bucket for reading
    and one encryption setting per bucket for writing
    """

    def __init__(
        self,
        read_bucket_settings: Dict[str, Optional[EncryptionSetting]],
        write_bucket_settings: Dict[str, Optional[EncryptionSetting]],
    ):
        self.read_bucket_settings = read_bucket_settings
        self.write_bucket_settings = write_bucket_settings

    def get_setting_for_upload(self, bucket, *args, **kwargs) -> Optional[EncryptionSetting]:
        return self.write_bucket_settings.get(bucket.name)

    def get_source_setting_for_copy(self, bucket, *args, **kwargs) -> None:
        return self.read_bucket_settings.get(bucket.name)

    def get_destination_setting_for_copy(self, bucket, *args,
                                         **kwargs) -> Optional[EncryptionSetting]:
        return self.write_bucket_settings.get(bucket.name)

    def get_setting_for_download(self, bucket, *args, **kwargs) -> None:
        return self.read_bucket_settings.get(bucket.name)

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.bucket_settings)
