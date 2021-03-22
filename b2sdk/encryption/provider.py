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

from .setting import EncryptionSetting


class AbstractEncryptionSettingsProvider(metaclass=ABCMeta):
    """
    Object which provides an appropriate EncryptionSetting object
    for complex operations with multiple sources and destinations such
    as sync and create_file
    """

    @abstractmethod
    def get_setting_for_upload(
        self,
        bucket,
        b2_file_name: str,
        file_info: dict,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for uploading an object or None if server should decide

        WARNING: the signature of this method is not final yet and not part of the public interface
        """

    @abstractmethod
    def get_source_setting_for_copy(
        self,
        bucket,
        file_id: str,
        dest_b2_file_name: str,
        length: int,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for source of copying an object or None if not required

        WARNING: the signature of this method is not final yet and not part of the public interface
        """

    @abstractmethod
    def get_destination_setting_for_copy(
        self,
        bucket,
        file_id: str,
        dest_b2_file_name: str,
        length: int,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for destination for copying an object or None if server should decide

        WARNING: the signature of this method is not final yet and not part of the public interface
        """

    @abstractmethod
    def get_setting_for_download(
        self,
        bucket,
        b2_file_name,
        file_id,
        mod_time_millis,
        file_size,
    ) -> Optional[EncryptionSetting]:
        """
        Return an EncryptionSetting for downloading an object from, or None if not required

        WARNING: the signature of this method is not final yet and not part of the public interface
        """


class ServerDefaultEncryptionSettingsProvider(AbstractEncryptionSettingsProvider):
    """
    Encryption settings provider which assumes setting-less reads
    and a bucket default for writes.

    As of 2021-03-18 this means either encryption or SSE-B2
    """

    def get_setting_for_upload(self, *args, **kwargs) -> None:
        return None

    def get_source_setting_for_copy(self, *args, **kwargs) -> None:
        return None

    def get_destination_setting_for_copy(self, *args, **kwargs) -> None:
        return None

    def get_setting_for_download(self, *args, **kwargs) -> None:
        return None


SERVER_DEFAULT_ENCRYPTION_SETTINGS_PROVIDER = ServerDefaultEncryptionSettingsProvider()


class BasicEncryptionSettingsProvider(AbstractEncryptionSettingsProvider):
    """
    Basic encryption setting provider that supports exactly one encryption setting per bucket

    WARNING: This class can be used by B2CLI for SSE-B2, but it's still in development
    """

    def __init__(self, bucket_settings: Dict[str, EncryptionSetting]):
        """
        :param dict bucket_settings: a mapping from bucket name to EncryptionSetting object
        """
        self.bucket_settings = bucket_settings

    def get_setting_for_upload(self, bucket, *args, **kwargs) -> Optional[EncryptionSetting]:
        return self.bucket_settings[bucket.name]

    def get_source_setting_for_copy(self, *args, **kwargs) -> None:
        """ signature and code TBD """
        raise NotImplementedError('no SSE-C support yet')

    def get_destination_setting_for_copy(self, bucket, *args,
                                         **kwargs) -> Optional[EncryptionSetting]:
        return self.bucket_settings[bucket.name]

    def get_setting_for_download(self, *args, **kwargs) -> None:
        """ signature and code TBD """
        raise NotImplementedError('no SSE-C support yet')

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.bucket_settings)
