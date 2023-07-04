######################################################################
#
# File: b2sdk/utils/typing.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from typing import TypeVar

from b2sdk.transfer.outbound.upload_source import AbstractUploadSource

_TypeUploadSource = TypeVar("_TypeUploadSource", bound=AbstractUploadSource)
