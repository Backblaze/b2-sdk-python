######################################################################
#
# File: b2sdk/v1/b2http.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import requests

from b2sdk import v2


# Overridden to retain old-style __init__ signature
class B2Http(v2.B2Http):
    """
    A wrapper for the requests module.  Provides the operations
    needed to access B2, and handles retrying when the returned
    status is 503 Service Unavailable, 429 Too Many Requests, etc.

    The operations supported are:

    - post_json_return_json
    - post_content_return_json
    - get_content

    The methods that return JSON either return a Python dict or
    raise a subclass of B2Error.  They can be used like this:

    .. code-block:: python

       try:
           response_dict = b2_http.post_json_return_json(url, headers, params)
           ...
       except B2Error as e:
           ...

    """

    # timeout for HTTP GET/POST requests
    TIMEOUT = 1200  # 20 minutes as server-side copy can take time

    def __init__(self, requests_module=None, install_clock_skew_hook=True, user_agent_append=None):
        """
        Initialize with a reference to the requests module, which makes
        it easy to mock for testing.

        :param requests_module: a reference to requests module
        :param bool install_clock_skew_hook: if True, install a clock skew hook
        :param str user_agent_append: if provided, the string will be appended to the User-Agent
        """
        super().__init__(
            v2.B2HttpApiConfig(
                http_session_factory=(requests_module or requests).Session,
                install_clock_skew_hook=install_clock_skew_hook,
                user_agent_append=user_agent_append
            )
        )
