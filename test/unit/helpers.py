######################################################################
#
# File: test/unit/helpers.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import inspect
from typing import Set


def list_abstract_methods(cls: type) -> Set[str]:
    return {
        name
        for name, value in inspect.getmembers(cls)
        if inspect.isfunction(value) and getattr(value, '__isabstractmethod__', False)
    }
