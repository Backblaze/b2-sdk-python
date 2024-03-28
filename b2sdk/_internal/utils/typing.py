######################################################################
#
# File: b2sdk/_internal/utils/typing.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from typing import Dict, List, Union

try:
    from typing_extensions import TypeAlias
except ImportError:
    from typing import TypeAlias

JSON: TypeAlias = Union[Dict[str, "JSON"], List["JSON"], str, int, float, bool, None]
