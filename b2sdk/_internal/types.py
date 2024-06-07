######################################################################
#
# File: b2sdk/_internal/types.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Types compatibility layer.

We use this module to support pydantic-less installs, as well as native typing module us on newer python versions.
"""

try:
    from typing_extensions import Annotated, NotRequired, TypedDict
except ImportError:
    from typing import Annotated, NotRequired, TypedDict

__all__ = [  # prevents linter from removing "unused imports" which we want to export
    "NotRequired",
    "PositiveInt",
    "TypedDict",
    "pydantic",
]

try:
    import pydantic

    if getattr(pydantic, "__version__", "") < "2":
        raise ImportError
except ImportError:
    pydantic = None

if pydantic:
    PositiveInt = Annotated[int, pydantic.Field(gte=0)]
else:
    PositiveInt = int
