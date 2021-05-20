######################################################################
#
# File: b2sdk/large_file/unfinished_large_file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.encryption.setting import EncryptionSettingFactory
from b2sdk.file_lock import FileRetentionSetting, LegalHold


class UnfinishedLargeFile(object):
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.file_id: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    :ivar str ~.account_id: account ID
    :ivar str ~.bucket_id: bucket ID
    :ivar str ~.content_type: :rfc:`822` content type, for example ``"application/octet-stream"``
    :ivar dict ~.file_info: file info dict
    """

    def __init__(self, file_dict):
        """
        Initialize from one file returned by ``b2_start_large_file`` or ``b2_list_unfinished_large_files``.
        """
        self.file_id = file_dict['fileId']
        self.file_name = file_dict['fileName']
        self.account_id = file_dict['accountId']
        self.bucket_id = file_dict['bucketId']
        self.content_type = file_dict['contentType']
        self.file_info = file_dict['fileInfo']
        self.encryption = EncryptionSettingFactory.from_file_version_dict(file_dict)
        self.file_retention = FileRetentionSetting.from_file_version_dict(file_dict)
        self.legal_hold = LegalHold.from_file_version_dict(file_dict)

    def __repr__(self):
        return '<%s %s %s>' % (self.__class__.__name__, self.bucket_id, self.file_name)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
