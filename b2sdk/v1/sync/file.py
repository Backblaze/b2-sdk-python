######################################################################
#
# File: b2sdk/v1/sync/file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod
import functools

from b2sdk import _v2 as v2
from .scan_policies import DEFAULT_SCAN_MANAGER
from .. import exception

FileVersion = v2.LocalFileVersion
