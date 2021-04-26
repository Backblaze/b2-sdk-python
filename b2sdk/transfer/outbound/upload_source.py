######################################################################
#
# File: b2sdk/transfer/outbound/upload_source.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import io
import os

from abc import abstractmethod

from b2sdk.exception import InvalidUploadSource
from b2sdk.stream.range import RangeOfInputStream, wrap_with_range
from b2sdk.transfer.outbound.outbound_source import OutboundTransferSource
from b2sdk.utils import hex_sha1_of_stream, hex_sha1_of_unlimited_stream


class AbstractUploadSource(OutboundTransferSource):
    """
    The source of data for uploading to b2.
    """

    @abstractmethod
    def get_content_sha1(self):
        """
        Return a 40-character string containing the hex SHA1 checksum of the data in the file.
        """

    @abstractmethod
    def open(self):
        """
        Return a binary file-like object from which the
        data can be read.
        :return:
        """

    def is_upload(self):
        return True

    def is_copy(self):
        return False

    def is_sha1_known(self):
        return False


class UploadSourceBytes(AbstractUploadSource):
    def __init__(self, data_bytes, content_sha1=None):
        self.data_bytes = data_bytes
        self.content_sha1 = content_sha1

    def __repr__(self):
        return '<{classname} data={data} id={id}>'.format(
            classname=self.__class__.__name__,
            data=str(self.data_bytes[:20]) +
            '...' if len(self.data_bytes) > 20 else self.data_bytes,
            id=id(self),
        )

    def get_content_length(self):
        return len(self.data_bytes)

    def get_content_sha1(self):
        if self.content_sha1 is None:
            self.content_sha1 = hashlib.sha1(self.data_bytes).hexdigest()
        return self.content_sha1

    def open(self):
        return io.BytesIO(self.data_bytes)

    def is_sha1_known(self):
        return self.content_sha1 is not None


class UploadSourceLocalFile(AbstractUploadSource):
    def __init__(self, local_path, content_sha1=None):
        self.local_path = local_path
        self.content_length = 0
        self.check_path_and_get_size()

        self.content_sha1 = content_sha1

    def check_path_and_get_size(self):
        if not os.path.isfile(self.local_path):
            raise InvalidUploadSource(self.local_path)
        self.content_length = os.path.getsize(self.local_path)

    def __repr__(self):
        return (
            '<{classname} local_path={local_path} content_length={content_length} '
            'content_sha1={content_sha1} id={id}>'
        ).format(
            classname=self.__class__.__name__,
            local_path=self.local_path,
            content_length=self.content_length,
            content_sha1=self.content_sha1,
            id=id(self),
        )

    def get_content_length(self):
        return self.content_length

    def get_content_sha1(self):
        if self.content_sha1 is None:
            self.content_sha1 = self._hex_sha1_of_file(self.local_path)
        return self.content_sha1

    def open(self):
        return io.open(self.local_path, 'rb')

    def _hex_sha1_of_file(self, local_path):
        with self.open() as f:
            return hex_sha1_of_stream(f, self.content_length)

    def is_sha1_known(self):
        return self.content_sha1 is not None


class UploadSourceLocalFileRange(UploadSourceLocalFile):
    def __init__(self, local_path, content_sha1=None, offset=0, length=None):
        super(UploadSourceLocalFileRange, self).__init__(local_path, content_sha1)
        self.file_size = self.content_length
        self.offset = offset
        if length is None:
            self.content_length = self.file_size - self.offset
        else:
            if length + self.offset > self.file_size:
                raise ValueError('Range length overflow file size')
            self.content_length = length

    def __repr__(self):
        return (
            '<{classname} local_path={local_path} offset={offset} '
            'content_length={content_length} content_sha1={content_sha1} id={id}>'
        ).format(
            classname=self.__class__.__name__,
            local_path=self.local_path,
            offset=self.offset,
            content_length=self.content_length,
            content_sha1=self.content_sha1,
            id=id(self),
        )

    def open(self):
        fp = super(UploadSourceLocalFileRange, self).open()
        return wrap_with_range(fp, self.file_size, self.offset, self.content_length)


class UploadSourceStream(AbstractUploadSource):
    def __init__(self, stream_opener, stream_length=None, stream_sha1=None):
        self.stream_opener = stream_opener
        self._content_length = stream_length
        self._content_sha1 = stream_sha1

    def __repr__(self):
        return (
            '<{classname} stream_opener={stream_opener} content_length={content_length} '
            'content_sha1={content_sha1} id={id}>'
        ).format(
            classname=self.__class__.__name__,
            stream_opener=repr(self.stream_opener),
            content_length=self._content_length,
            content_sha1=self._content_sha1,
            id=id(self),
        )

    def get_content_length(self):
        if self._content_length is None:
            self._set_content_length_and_sha1()
        return self._content_length

    def get_content_sha1(self):
        if self._content_sha1 is None:
            self._set_content_length_and_sha1()
        return self._content_sha1

    def open(self):
        return self.stream_opener()

    def _set_content_length_and_sha1(self):
        sha1, content_length = hex_sha1_of_unlimited_stream(self.open())
        self._content_length = content_length
        self._content_sha1 = sha1

    def is_sha1_known(self):
        return self._content_sha1 is not None


class UploadSourceStreamRange(UploadSourceStream):
    def __init__(self, stream_opener, offset, stream_length, stream_sha1=None):
        super(UploadSourceStreamRange, self).__init__(
            stream_opener,
            stream_length=stream_length,
            stream_sha1=stream_sha1,
        )
        self._offset = offset

    def __repr__(self):
        return (
            '<{classname} stream_opener={stream_opener} offset={offset} '
            'content_length={content_length} content_sha1={content_sha1} id={id}>'
        ).format(
            classname=self.__class__.__name__,
            stream_opener=repr(self.stream_opener),
            offset=self._offset,
            content_length=self._content_length,
            content_sha1=self._content_sha1,
            id=id(self),
        )

    def open(self):
        return RangeOfInputStream(
            super(UploadSourceStreamRange, self).open(), self._offset, self._content_length
        )
