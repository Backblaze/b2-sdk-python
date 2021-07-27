######################################################################
#
# File: b2sdk/b2http_adapter.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from requests.adapters import HTTPAdapter

from .requests import NotDecompressingResponse


class NotDecompressingHTTPAdapter(HTTPAdapter):
    """
    HTTP adapter that uses :class:`b2sdk.requests.NotDecompressingResponse` instead of the default
    :code:`requests.Response` class.
    """

    def build_response(self, req, resp):
        return NotDecompressingResponse.from_builtin_response(super().build_response(req, resp))
