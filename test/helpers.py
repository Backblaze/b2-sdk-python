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
