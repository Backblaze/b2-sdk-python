######################################################################
#
# File: b2sdk/transfer/inbound/downloader/abstract.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod

from b2sdk.utils import B2TraceMetaAbstract

from .range import Range


class AbstractDownloader(metaclass=B2TraceMetaAbstract):
    def __init__(
        self,
        force_chunk_size=None,
        min_chunk_size=None,
        max_chunk_size=None,
    ):
        assert force_chunk_size is not None or (
            min_chunk_size is not None and max_chunk_size is not None and min_chunk_size > 0 and
            max_chunk_size >= min_chunk_size
        )
        self._min_chunk_size = min_chunk_size
        self._max_chunk_size = max_chunk_size
        self._forced_chunk_size = force_chunk_size

    def _get_chunk_size(self, content_length):
        if self._forced_chunk_size is not None:
            return self._forced_chunk_size
        ideal = content_length // 1000
        non_aligned = min(max(ideal, self._min_chunk_size), self._max_chunk_size)
        aligned = non_aligned // 4096 * 4096
        return aligned

    @classmethod
    def _get_remote_range(cls, response, metadata):
        """
        Get a range from response or original request (as appropriate).

        :param response: requests.Response of initial request
        :param metadata: metadata dict of the target file
        :return: a range object
        """
        raw_range_header = response.request.headers.get('Range')  # 'bytes 0-11'
        if raw_range_header is None:
            return Range(0, 0 if metadata.content_length == 0 else metadata.content_length - 1)
        return Range.from_header(raw_range_header)

    @abstractmethod
    def is_suitable(self, metadata, progress_listener):
        """
        Analyze metadata (possibly against options passed earlier to constructor
        to find out whether the given download request should be handled by this downloader).
        """
        pass

    @abstractmethod
    def download(
        self,
        file,
        response,
        metadata,
        session,
    ):
        """
        @returns (bytes_read, actual_sha1)
        """
        pass
