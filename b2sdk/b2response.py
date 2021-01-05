######################################################################
#
# File: b2sdk/b2response.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from requests.exceptions import ChunkedEncodingError, ContentDecodingError, ConnectionError, StreamConsumedError
from requests.models import Response
from requests.utils import iter_slices, stream_decode_response_unicode
from urllib3.exceptions import ProtocolError, DecodeError, ReadTimeoutError


class B2Response(Response):
    """
    Child class of 'Response' from module 'Requests.'

    Overrides method 'iter_content' to set 'decode_content' to 'False'
    in the call to self.raw.stream() in order to avoid decompressing files
    that have been uploaded with 'b2-content-encoding=gzip'. Up-to-date with
    v2.25.1 of Requests library (2020-12-16).
    """

    def iter_content(self, chunk_size=1, decode_unicode=False):
        """Iterates over the response data.  When stream=True is set on the
        request, this avoids reading the content at once into memory for
        large responses.  The chunk size is the number of bytes it should
        read into memory.  This is not necessarily the length of each item
        returned as decoding can take place.

        chunk_size must be of type int or None. A value of None will
        function differently depending on the value of `stream`.
        stream=True will read data as it arrives in whatever size the
        chunks are received. If stream=False, data is returned as
        a single chunk.

        If decode_unicode is True, content will be decoded using the best
        available encoding based on the response.
        """

        def generate():
            # Special case for urllib3.
            if hasattr(self.raw, 'stream'):
                try:
                    # set decode_content to False to prevent decompressing files that
                    # have been uploaded with b2-content-encoding=gzip
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