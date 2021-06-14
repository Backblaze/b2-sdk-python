######################################################################
#
# File: b2sdk/api_config.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import requests

from typing import Optional

from types import ModuleType


class B2HttpApiConfig:
    def __init__(
        self,
        requests_module: ModuleType = requests,
        install_clock_skew_hook: bool = True,
        user_agent_append: Optional[str] = None,
    ):
        """
        A structure with params to be passed to low level API.

        :param requests_module: a reference to requests module
        :param bool install_clock_skew_hook: if True, install a clock skew hook
        :param str user_agent_append: if provided, the string will be appended to the User-Agent
        """
        self.requests_module = requests_module
        self.install_clock_skew_hook = install_clock_skew_hook
        self.user_agent_append = user_agent_append


DEFAULT_HTTP_API_CONFIG = B2HttpApiConfig()
