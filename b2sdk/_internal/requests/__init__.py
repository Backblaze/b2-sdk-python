######################################################################
#
# File: b2sdk/_internal/requests/__init__.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
# Copyright 2019 Kenneth Reitz
#
# License https://www.backblaze.com/using_b2_code.html
# License Apache License 2.0 (http://www.apache.org/licenses/ and LICENSE file in this directory)
#
######################################################################
"""This file contains modified parts of the requests module (https://github.com/psf/requests, models.py), original
Copyright 2019 Kenneth Reitz

Changes made to the original source: see NOTICE
"""

from requests import Response, ConnectionError
from requests.exceptions import ChunkedEncodingError, ContentDecodingError, StreamConsumedError
from requests.utils import iter_slices, stream_decode_response_unicode
from urllib3.exceptions import ProtocolError, DecodeError, ReadTimeoutError

from . import included_source_meta


class NotDecompressingResponse(Response):
    def iter_content(self, chunk_size=1, decode_unicode=False):
        def generate():
            # Special case for urllib3.
            if hasattr(self.raw, 'stream'):
                try:
                    # set decode_content to False to prevent decompressing files that
                    # when Content-Encoding response header is set
                    for chunk in self.raw.stream(chunk_size, decode_content=False):
                        yield chunk
                except ProtocolError as e:
                    raise ChunkedEncodingError(e)
                except DecodeError as e:
                    raise ContentDecodingError(e)
                except ReadTimeoutError as e:
                    raise ConnectionError(e)
            else:
                # Standard file-like object.
                while True:
                    chunk = self.raw.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

            self._content_consumed = True

        if self._content_consumed and isinstance(self._content, bool):
            raise StreamConsumedError()
        elif chunk_size is not None and not isinstance(chunk_size, int):
            raise TypeError("chunk_size must be an int, it is instead a %s." % type(chunk_size))
        # simulate reading small chunks of the content
        reused_chunks = iter_slices(self._content, chunk_size)

        stream_chunks = generate()

        chunks = reused_chunks if self._content_consumed else stream_chunks

        if decode_unicode:
            chunks = stream_decode_response_unicode(chunks, self)

        return chunks

    @classmethod
    def from_builtin_response(cls, response: Response):
        """
        Create a :class:`b2sdk._internal.requests.NotDecompressingResponse` object from a :class:`requests.Response` object.
        Don't use :code:`Response.__getstate__` and :code:`Response.__setstate__`
        because these assume that the content has been consumed, which will never be true in our case.
        """
        new_response = cls()
        for attr_name in cls.__attrs__:
            setattr(new_response, attr_name, getattr(response, attr_name))
        new_response.raw = response.raw
        return new_response
