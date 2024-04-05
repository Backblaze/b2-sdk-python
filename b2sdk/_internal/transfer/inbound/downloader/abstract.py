######################################################################
#
# File: b2sdk/_internal/transfer/inbound/downloader/abstract.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from io import IOBase

from requests.models import Response

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.file_version import DownloadVersion
from b2sdk._internal.session import B2Session
from b2sdk._internal.utils import B2TraceMetaAbstract
from b2sdk._internal.utils.range_ import Range


class EmptyHasher:
    def __init__(self, *args, **kwargs):
        pass

    def update(self, data):
        pass

    def digest(self):
        return b''

    def hexdigest(self):
        return ''

    def copy(self):
        return self


class AbstractDownloader(metaclass=B2TraceMetaAbstract):
    """
    Abstract class for downloaders.

    :var REQUIRES_SEEKING: if True, the downloader requires the ability to seek in the file object.
    :var SUPPORTS_DECODE_CONTENT: if True, the downloader supports decoded HTTP streams.
        In practice, this means that the downloader can handle HTTP responses which already
        have the content decoded per Content-Encoding and, more likely than not, of a different
        length than requested.
    """

    REQUIRES_SEEKING = True
    SUPPORTS_DECODE_CONTENT = True
    DEFAULT_THREAD_POOL_CLASS = staticmethod(ThreadPoolExecutor)
    DEFAULT_ALIGN_FACTOR = 4096

    def __init__(
        self,
        thread_pool: ThreadPoolExecutor | None = None,
        force_chunk_size: int | None = None,
        min_chunk_size: int | None = None,
        max_chunk_size: int | None = None,
        align_factor: int | None = None,
        check_hash: bool = True,
        **kwargs
    ):
        align_factor = align_factor or self.DEFAULT_ALIGN_FACTOR
        assert force_chunk_size is not None or (
            min_chunk_size is not None and max_chunk_size is not None and
            0 < min_chunk_size <= max_chunk_size and max_chunk_size >= align_factor
        )
        self._min_chunk_size = min_chunk_size
        self._max_chunk_size = max_chunk_size
        self._forced_chunk_size = force_chunk_size
        self._align_factor = align_factor
        self._check_hash = check_hash
        self._thread_pool = thread_pool if thread_pool is not None \
            else self.DEFAULT_THREAD_POOL_CLASS()
        super().__init__(**kwargs)

    def _get_hasher(self):
        if self._check_hash:
            return hashlib.sha1()
        return EmptyHasher()

    def _get_chunk_size(self, content_length: int | None):
        if self._forced_chunk_size is not None:
            return self._forced_chunk_size
        ideal = max(content_length // 1000, self._align_factor)
        non_aligned = min(max(ideal, self._min_chunk_size), self._max_chunk_size)
        aligned = non_aligned // self._align_factor * self._align_factor
        return aligned

    @classmethod
    def _get_remote_range(cls, response: Response, download_version: DownloadVersion):
        """
        Get a range from response or original request (as appropriate).

        :param response: requests.Response of initial request
        :param download_version: b2sdk.v2.DownloadVersion
        :return: a range object
        """
        if 'Range' in response.request.headers:
            return Range.from_header(response.request.headers['Range'])
        return download_version.range_

    def is_suitable(self, download_version: DownloadVersion, allow_seeking: bool):
        """
        Analyze download_version (possibly against options passed earlier to constructor
        to find out whether the given download request should be handled by this downloader).
        """
        if self.REQUIRES_SEEKING and not allow_seeking:
            return False
        if not self.SUPPORTS_DECODE_CONTENT and download_version.content_encoding and download_version.api.api_config.decode_content:
            return False
        return True

    @abstractmethod
    def download(
        self,
        file: IOBase,
        response: Response,
        download_version: DownloadVersion,
        session: B2Session,
        encryption: EncryptionSetting | None = None,
    ) -> tuple[int, str]:
        """
        Download target to a file-like object.

        :param file: file-like object to write to
        :param response: requests.Response of b2_download_url_by_* endpoint with the target object
        :param download_version: DownloadVersion of an object being downloaded
        :param session: B2Session to be used for downloading
        :param encryption: optional Encryption setting
        :return: (bytes_read, actual_sha1)
            please note bytes_read may be different from bytes written to a file object if decode_content=True
        """
        pass
