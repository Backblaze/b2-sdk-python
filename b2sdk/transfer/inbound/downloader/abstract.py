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
from io import IOBase
from typing import Optional

from requests.models import Response

from b2sdk.file_version import DownloadVersion
from b2sdk.session import B2Session
from b2sdk.utils import B2TraceMetaAbstract
from b2sdk.utils.range_ import Range
from b2sdk.encryption.setting import EncryptionSetting


class AbstractDownloader(metaclass=B2TraceMetaAbstract):

    REQUIRES_SEEKING = True

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
    def _get_remote_range(cls, response: Response, download_version: DownloadVersion):
        """
        Get a range from response or original request (as appropriate).

        :param response: requests.Response of initial request
        :param download_version: b2sdk.v1.DownloadVersion
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
        return True

    @abstractmethod
    def download(
        self,
        file: IOBase,
        response: Response,
        download_version: DownloadVersion,
        session: B2Session,
        encryption: Optional[EncryptionSetting] = None,
    ):
        """
        @returns (bytes_read, actual_sha1)
        """
        pass
