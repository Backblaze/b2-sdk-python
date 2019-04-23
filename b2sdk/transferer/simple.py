######################################################################
#
# File: b2sdk/transferer/simple.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import logging

from .abstract import AbstractDownloader

logger = logging.getLogger(__name__)


class SimpleDownloader(AbstractDownloader):
    def __init__(self, *args, **kwargs):
        super(SimpleDownloader, self).__init__(*args, **kwargs)

    def is_suitable(self, metadata, progress_listener):
        return True

    def download(self, file, response, metadata, session):
        actual_size = self._get_remote_range(response, metadata).size()
        chunk_size = self._get_chunk_size(actual_size)

        digest = hashlib.sha1()
        bytes_read = 0
        for data in response.iter_content(chunk_size=chunk_size):
            file.write(data)
            digest.update(data)
            bytes_read += len(data)

        assert actual_size >= 1  # code below does `actual_size - 1`, but it should never reach that part with an empty file

        # now normally bytes_read == metadata.content_length but sometimes there is a timeout
        # or something and the server closes connection, while neither tcp or http have a problem
        # with the truncated output, so we detect it here and try to continue

        num_tries = 5  # this is hardcoded because we are going to replace the entire retry interface soon, so we'll avoid deprecation here and keep it private
        retries_left = num_tries - 1
        while retries_left and bytes_read < metadata.content_length:
            new_range = self._get_remote_range(
                response,
                metadata,
            ).subrange(bytes_read, actual_size - 1)
            # original response is not closed at this point yet, as another layer is responsible for closing it, so a new socket might be allocated,
            # but this is a very rare case and so not worth the optimization
            logger.debug(
                're-download attempts remaining: %i, bytes read already: %i. Getting range %s now.',
                retries_left, bytes_read, new_range
            )
            with session.download_file_from_url(
                response.request.url,
                new_range.as_tuple(),
            ) as followup_response:
                for data in followup_response.iter_content(
                    chunk_size=self._get_chunk_size(actual_size)
                ):
                    file.write(data)
                    digest.update(data)
                    bytes_read += len(data)
            retries_left -= 1
        return bytes_read, digest.hexdigest()
