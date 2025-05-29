######################################################################
#
# File: b2sdk/v2/api_config.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from b2sdk import v3
from .raw_api import B2RawHTTPApi


class B2HttpApiConfig(v3.B2HttpApiConfig):
    DEFAULT_RAW_API_CLASS = B2RawHTTPApi


DEFAULT_HTTP_API_CONFIG = B2HttpApiConfig()
