######################################################################
#
# File: b2sdk/_internal/transfer/inbound/downloader/simple.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
from io import IOBase

from requests.models import Response

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.file_version import DownloadVersion
from b2sdk._internal.session import B2Session

from .abstract import AbstractDownloader

logger = logging.getLogger(__name__)


class SimpleDownloader(AbstractDownloader):

    REQUIRES_SEEKING = False
    SUPPORTS_DECODE_CONTENT = True

    def _download(
        self,
        file: IOBase,
        response: Response,
        download_version: DownloadVersion,
        session: B2Session,
        encryption: EncryptionSetting | None = None,
    ):
        digest = self._get_hasher()
        actual_size = self._get_remote_range(response, download_version).size()
        if actual_size == 0:
            response.close()
            return 0, digest.hexdigest()
        chunk_size = self._get_chunk_size(actual_size)

        decoded_bytes_read = 0
        for data in response.iter_content(chunk_size=chunk_size):
            file.write(data)
            digest.update(data)
            decoded_bytes_read += len(data)
        bytes_read = response.raw.tell()
        response.close()

        assert actual_size >= 1  # code below does `actual_size - 1`, but it should never reach that part with an empty file

        # now, normally bytes_read == download_version.content_length, but sometimes there is a timeout
        # or something and the server closes connection, while neither tcp or http have a problem
        # with the truncated output, so we detect it here and try to continue

        num_tries = 5  # this is hardcoded because we are going to replace the entire retry interface soon, so we'll avoid deprecation here and keep it private
        retries_left = num_tries - 1
        while retries_left and bytes_read < download_version.content_length:
            new_range = self._get_remote_range(
                response,
                download_version,
            ).subrange(bytes_read, actual_size - 1)
            # original response is not closed at this point yet, as another layer is responsible for closing it, so a new socket might be allocated,
            # but this is a very rare case and so it is not worth the optimization
            logger.debug(
                're-download attempts remaining: %i, bytes read: %i (decoded: %i). Getting range %s now.',
                retries_left, bytes_read, decoded_bytes_read, new_range
            )
            with session.download_file_from_url(
                response.request.url,
                new_range.as_tuple(),
                encryption=encryption,
            ) as followup_response:
                for data in followup_response.iter_content(
                    chunk_size=self._get_chunk_size(actual_size)
                ):
                    file.write(data)
                    digest.update(data)
                    decoded_bytes_read += len(data)
                bytes_read += followup_response.raw.tell()
            retries_left -= 1
        return bytes_read, digest.hexdigest()

    def download(
        self,
        file: IOBase,
        response: Response,
        download_version: DownloadVersion,
        session: B2Session,
        encryption: EncryptionSetting | None = None,
    ):
        future = self._thread_pool.submit(
            self._download, file, response, download_version, session, encryption
        )
        return future.result()
