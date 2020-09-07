######################################################################
#
# File: b2sdk/transfer/inbound/file_metadata.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class FileMetadata(object):
    """
    Hold information about a file which is being downloaded.
    """
    UNVERIFIED_CHECKSUM_PREFIX = 'unverified:'

    __slots__ = (
        'file_id',
        'file_name',
        'content_type',
        'content_length',
        'content_sha1',
        'content_sha1_verified',
        'file_info',
    )

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

    @classmethod
    def from_response(cls, response):
        info = response.headers
        return cls(
            file_id=info['x-bz-file-id'],
            file_name=info['x-bz-file-name'],
            content_type=info['content-type'],
            content_length=int(info['content-length']),
            content_sha1=info['x-bz-content-sha1'],
            file_info=dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-')),
        )

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
