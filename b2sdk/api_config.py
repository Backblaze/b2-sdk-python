######################################################################
#
# File: b2sdk/api_config.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional, Callable, Type
import requests

from .raw_api import AbstractRawApi, B2RawHTTPApi


class B2HttpApiConfig:

    DEFAULT_RAW_API_CLASS = B2RawHTTPApi

    def __init__(
        self,
        http_session_factory: Callable[[], requests.Session] = requests.Session,
        install_clock_skew_hook: bool = True,
        user_agent_append: Optional[str] = None,
        _raw_api_class: Optional[Type[AbstractRawApi]] = None,
    ):
        """
        A structure with params to be passed to low level API.

        :param http_session_factory: a callable that returns a requests.Session object (or a compatible one)
        :param install_clock_skew_hook: if True, install a clock skew hook
        :param user_agent_append: if provided, the string will be appended to the User-Agent
        :param _raw_api_class: AbstractRawApi-compliant class
        """
        self.http_session_factory = http_session_factory
        self.install_clock_skew_hook = install_clock_skew_hook
        self.user_agent_append = user_agent_append
        self.raw_api_class = _raw_api_class or self.DEFAULT_RAW_API_CLASS


DEFAULT_HTTP_API_CONFIG = B2HttpApiConfig()
