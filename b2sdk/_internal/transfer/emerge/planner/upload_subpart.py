######################################################################
#
# File: b2sdk/_internal/transfer/emerge/planner/upload_subpart.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io
from abc import ABCMeta, abstractmethod
from functools import partial

from b2sdk._internal.stream.chained import StreamOpener
from b2sdk._internal.stream.range import wrap_with_range
from b2sdk._internal.utils import hex_sha1_of_unlimited_stream


class BaseUploadSubpart(metaclass=ABCMeta):
    def __init__(self, outbound_source, relative_offset, length):
        self.outbound_source = outbound_source
        self.relative_offset = relative_offset
        self.length = length

    def __repr__(self):
        return (
            '<{classname} outbound_source={outbound_source} relative_offset={relative_offset} '
            'length={length}>'
        ).format(
            classname=self.__class__.__name__,
            outbound_source=repr(self.outbound_source),
            relative_offset=self.relative_offset,
            length=self.length,
        )

    @abstractmethod
    def get_subpart_id(self):
        pass

    @abstractmethod
    def get_stream_opener(self, emerge_execution=None):
        pass

    def is_hashable(self):
        return False


class RemoteSourceUploadSubpart(BaseUploadSubpart):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._download_buffer_cache = None

    def get_subpart_id(self):
        return (self.outbound_source.file_id, self.relative_offset, self.length)

    def get_stream_opener(self, emerge_execution=None):
        if emerge_execution is None:
            raise RuntimeError('Cannot open remote source without emerge execution instance.')
        return CachedBytesStreamOpener(partial(self._download, emerge_execution))

    def _download(self, emerge_execution):
        url = emerge_execution.services.session.get_download_url_by_id(self.outbound_source.file_id)
        absolute_offset = self.outbound_source.offset + self.relative_offset
        range_ = (absolute_offset, absolute_offset + self.length - 1)
        with io.BytesIO() as bytes_io:
            downloaded_file = emerge_execution.services.download_manager.download_file_from_url(
                url, range_=range_, encryption=self.outbound_source.encryption
            )
            downloaded_file.save(bytes_io)
            return bytes_io.getvalue()


class LocalSourceUploadSubpart(BaseUploadSubpart):
    def get_subpart_id(self):
        with self._get_stream() as stream:
            sha1, _ = hex_sha1_of_unlimited_stream(stream)
            return sha1

    def get_stream_opener(self, emerge_execution=None):
        return self._get_stream

    def _get_stream(self):
        fp = self.outbound_source.open()
        return wrap_with_range(
            fp, self.outbound_source.get_content_length(), self.relative_offset, self.length
        )

    def is_hashable(self):
        return True


class CachedBytesStreamOpener(StreamOpener):
    def __init__(self, bytes_data_callback):
        self.bytes_data_callback = bytes_data_callback
        self._bytes_data_cache = None

    def __call__(self):
        if self._bytes_data_cache is None:
            self._bytes_data_cache = self.bytes_data_callback()
        return io.BytesIO(self._bytes_data_cache)

    def cleanup(self):
        self._bytes_data_cache = None
