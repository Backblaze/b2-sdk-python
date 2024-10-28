######################################################################
#
# File: test/helpers.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import contextlib
import inspect
import io
from unittest.mock import patch

from b2sdk._internal.types import pydantic


@contextlib.contextmanager
def patch_bind_params(instance, method_name):
    """
    Patch a method of instance.

    In addition to `patch.object(instance, method_name)` would provide, it also adds get_bound_call_args method
    on the returned mock.
    This allows to get the arguments that were passed to the method, after binding.

    :param instance: instance to patch
    :param method_name: name of the method of instance to patch
    :return: patched method mock
    """
    signature = inspect.signature(getattr(instance, method_name))
    with patch.object(instance, method_name, autospec=True) as mock_method:
        mock_method.get_bound_call_args = lambda: signature.bind(
            *mock_method.call_args[0], **mock_method.call_args[1]
        ).arguments
        yield mock_method


class NonSeekableIO(io.BytesIO):
    """Emulate a non-seekable file"""

    def seek(self, *args, **kwargs):
        raise OSError('not seekable')

    def seekable(self):
        return False


def type_validator_factory(type_):
    """
    Equivalent of `TypeAdapter(type_).validate_python` and noop under Python <3.8.

    To be removed when we drop support for Python <3.8.
    """
    if pydantic:
        return pydantic.TypeAdapter(type_).validate_python
    return lambda *args, **kwargs: None


def deep_cast_dict(actual, expected):
    """
    For composite objects `actual` and `expected`, return a copy of `actual` (with all dicts and lists deeply copied)
    with all keys of dicts not appearing in `expected` (comparing dicts on any level) removed. Useful for assertions
    in tests ignoring extra keys.
    """
    if isinstance(expected, dict) and isinstance(actual, dict):
        return {k: deep_cast_dict(actual[k], expected[k]) for k in expected if k in actual}

    elif isinstance(expected, list) and isinstance(actual, list):
        return [deep_cast_dict(a, e) for a, e in zip(actual, expected)]

    return actual


def assert_dict_equal_ignore_extra(actual, expected):
    assert deep_cast_dict(actual, expected) == expected
