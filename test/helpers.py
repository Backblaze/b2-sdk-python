######################################################################
#
# File: test/helpers.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import contextlib
import inspect
from unittest.mock import patch


@contextlib.contextmanager
def patch_bind_params(instance, method_name):
    signature = inspect.signature(getattr(instance, method_name))
    with patch.object(instance, method_name, autospec=True) as mock_method:
        mock_method.get_bound_call_args = lambda: signature.bind(
            *mock_method.call_args[0], **mock_method.call_args[1]
        ).arguments
        yield mock_method
