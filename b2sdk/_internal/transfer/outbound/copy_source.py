######################################################################
#
# File: b2sdk/_internal/transfer/outbound/copy_source.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.http_constants import LARGE_FILE_SHA1
from b2sdk._internal.transfer.outbound.outbound_source import OutboundTransferSource


class CopySource(OutboundTransferSource):
    def __init__(
        self,
        file_id,
        offset=0,
        length=None,
        encryption: EncryptionSetting | None = None,
        source_file_info=None,
        source_content_type=None,
    ):
        if not length and offset > 0:
            raise ValueError('Cannot copy with non zero offset and unknown or zero length')
        self.file_id = file_id
        self.length = length
        self.offset = offset
        self.encryption = encryption
        self.source_file_info = source_file_info
        self.source_content_type = source_content_type

    def __repr__(self):
        return (
            '<{classname} file_id={file_id} offset={offset} length={length} id={id}, encryption={encryption},'
            'source_content_type={source_content_type}>, source_file_info={source_file_info}'
        ).format(
            classname=self.__class__.__name__,
            file_id=self.file_id,
            offset=self.offset,
            length=self.length,
            id=id(self),
            encryption=self.encryption,
            source_content_type=self.source_content_type,
            source_file_info=self.source_file_info,
        )

    def get_content_length(self):
        return self.length

    def is_upload(self):
        return False

    def is_copy(self):
        return True

    def get_bytes_range(self):
        if not self.length:
            if self.offset > 0:
                # auto mode should get file info and create correct copy source (with length)
                raise ValueError(
                    'cannot return bytes range for non zero offset and unknown or zero length'
                )
            return None

        return self.offset, self.offset + self.length - 1

    def get_copy_source_range(self, relative_offset, range_length):
        if self.length is not None and range_length + relative_offset > self.length:
            raise ValueError('Range length overflow source length')
        range_offset = self.offset + relative_offset
        return self.__class__(
            self.file_id,
            range_offset,
            range_length,
            encryption=self.encryption,
            source_file_info=self.source_file_info,
            source_content_type=self.source_content_type
        )

    def get_content_sha1(self):
        if self.offset or self.length:
            # this is a copy of only a range of the source, can't copy the SHA1
            return None
        return self.source_file_info.get(LARGE_FILE_SHA1)
