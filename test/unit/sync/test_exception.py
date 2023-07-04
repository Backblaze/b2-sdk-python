######################################################################
#
# File: test/unit/sync/test_exception.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from apiver_deps_exception import (
    EnvironmentEncodingError,
    IncompleteSync,
    InvalidArgument,
    UnsupportedFilename,
    check_invalid_argument,
)


class TestSyncExceptions:
    def test_environment_encoding_error(self):
        try:
            raise EnvironmentEncodingError('fred', 'george')
        except EnvironmentEncodingError as e:
            assert str(e) == """file name fred cannot be decoded with system encoding (george).
We think this is an environment error which you should workaround by
setting your system encoding properly, for example like this:
export LANG=en_US.UTF-8""", str(e)

    def test_invalid_argument(self):
        try:
            raise InvalidArgument('param', 'message')
        except InvalidArgument as e:
            assert str(e) == 'param message', str(e)

    def test_incomplete_sync(self):
        try:
            raise IncompleteSync()
        except IncompleteSync as e:
            assert str(e) == 'Incomplete sync: ', str(e)

    def test_unsupportedfilename_error(self):
        try:
            raise UnsupportedFilename('message', 'filename')
        except UnsupportedFilename as e:
            assert str(e) == 'message: filename', str(e)


class TestCheckInvalidArgument:
    def test_custom_message(self):
        with pytest.raises(InvalidArgument):
            try:
                with check_invalid_argument('param', 'an error occurred', RuntimeError):
                    raise RuntimeError()
            except InvalidArgument as exc:
                assert str(exc) == 'param an error occurred'
                raise

    def test_message_from_exception(self):
        with pytest.raises(InvalidArgument):
            try:
                with check_invalid_argument('param', '', RuntimeError):
                    raise RuntimeError('an error occurred')
            except InvalidArgument as exc:
                assert str(exc) == 'param an error occurred'
                raise
