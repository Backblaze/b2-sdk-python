import io

from functools import partial

from b2sdk.download_dest import DownloadDestBytes
from b2sdk.stream.range import wrap_with_range


class BaseUploadSubpart(object):
    def __init__(self, outbound_source, relative_offset, length):
        self.outbound_source = outbound_source
        self.relative_offset = relative_offset
        self.length = length

    def __repr__(self):
        return ('<{classname} outbound_source={outbound_source} relative_offset={relative_offset} '
                'length={length}>').format(
            classname=self.__class__.__name__,
            outbound_source=repr(self.outbound_source),
            relative_offset=self.relative_offset,
            length=self.length,
        )

    def get_subpart_id(self):
        return (self.outbound_source.get_source_id(), self.relative_offset, self.length)

    def get_stream_opener(self, emerge_execution):
        raise NotImplementedError()

    def is_hashable(self):
        return False


class RemoteSourceUploadSubpart(BaseUploadSubpart):
    def __init__(self, outbound_source, relative_offset, length):
        super(RemoteSourceUploadSubpart, self).__init__(outbound_source, relative_offset, length)
        self._download_buffer_cache = None

    def get_stream_opener(self, emerge_execution):
        return partial(self._get_download_stream, emerge_execution)

    def _get_download_stream(self, emerge_execution):
        if self._download_buffer_cache is None:
            self._download_buffer_cache = self._download(emerge_execution)
        return io.BytesIO(self._download_buffer_cache)

    def _download(self, emerge_execution):
        url = emerge_execution.session.get_download_url_by_id(self.outbound_source.file_id)
        absolute_offset = self.outbound_source.offset + self.relative_offset
        download_dest = DownloadDestBytes()
        range_ = (absolute_offset, absolute_offset + self.length - 1)
        emerge_execution.services.download_manager.download_file_from_url(url, download_dest, range_=range_)
        return download_dest.get_bytes_written()


class LocalSourceUploadSubpart(BaseUploadSubpart):
    def get_subpart_id(self):
        return (self.outbound_source.get_source_id(), self.relative_offset, self.length)

    def get_stream_opener(self, emerge_execution):
        return self._get_stream

    def _get_stream(self):
        fp = self.outbound_source.open()
        return wrap_with_range(fp, self.outbound_source.get_content_length(), self.relative_offset, self.length)

    def is_hashable(self):
        return True
