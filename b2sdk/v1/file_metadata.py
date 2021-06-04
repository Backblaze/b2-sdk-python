######################################################################
#
# File: b2sdk/v1/file_metadata.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..file_version import FileVersion


class FileMetadata(object):
    """
    Hold information about a file which is being downloaded.
    """
    UNVERIFIED_CHECKSUM_PREFIX = 'unverified:'

    def __init__(
        self,
        file_id,
        file_name,
        content_type,
        content_length,
        content_sha1,
        file_info,
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_type = content_type
        self.content_length = content_length
        self.content_sha1, self.content_sha1_verified = self._decode_content_sha1(content_sha1)
        self.file_info = file_info

    def as_info_dict(self):
        return {
            'fileId': self.file_id,
            'fileName': self.file_name,
            'contentType': self.content_type,
            'contentLength': self.content_length,
            'contentSha1': self._encode_content_sha1(self.content_sha1, self.content_sha1_verified),
            'fileInfo': self.file_info,
        }

    @classmethod
    def _decode_content_sha1(cls, content_sha1):
        if content_sha1.startswith(cls.UNVERIFIED_CHECKSUM_PREFIX):
            return content_sha1[len(cls.UNVERIFIED_CHECKSUM_PREFIX):], False
        return content_sha1, True

    @classmethod
    def _encode_content_sha1(cls, content_sha1, content_sha1_verified):
        if not content_sha1_verified:
            return '%s%s' % (cls.UNVERIFIED_CHECKSUM_PREFIX, content_sha1)
        return content_sha1

    @classmethod
    def from_file_version(cls, file_version: FileVersion):
        return cls(
            file_id=file_version.id_,
            file_name=file_version.file_name,
            content_type=file_version.content_type,
            content_length=file_version.size,
            content_sha1=file_version.content_sha1,
            file_info=file_version.file_info,
        )
