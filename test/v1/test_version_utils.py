######################################################################
#
# File: test/test_version_utils.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import warnings

from .test_base import TestBase
from b2sdk.version import VERSION
from b2sdk.version_utils import rename_argument, rename_function


class TestRenameArgument(TestBase):
    def test_warning(self):
        @rename_argument('aaa', 'bbb', '0.1.0', '0.2.0')
        def easy(bbb):
            """ easy docstring """
            return bbb

        # check that warning is not emitted too early
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert easy(5) == 5
            assert easy(bbb=5) == 5
            assert easy.__name__ == 'easy'
            assert easy.__doc__ == ' easy docstring '
            assert len(w) == 0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert easy(aaa=5) == 5
            assert len(w) == 1
            assert issubclass(w[-1].category, DeprecationWarning)
            assert str(
                w[-1].message
            ) == "'aaa' is a deprecated argument for 'easy' function/method - it was renamed to 'bbb' in version 0.1.0. Support for the old name is going to be dropped in 0.2.0.", str(
                w[-1].message
            )

    def test_outdated_replacement(self):
        with self.assertRaises(
            AssertionError,
            msg=
            "rename_argument decorator is still used in version %s when old argument name 'aaa' was scheduled to be dropped in 0.1.2. It is time to remove the mapping."
            % (VERSION,),
        ):

            @rename_argument('aaa', 'bbb', '0.1.0', '0.1.2')
            def late(bbb):
                return bbb

            assert late  # make linters happy

    def test_future_replacement(self):
        with self.assertRaises(
            AssertionError,
            msg=
            "rename_argument decorator indicates that the replacement of argument 'aaa' should take place in the future version 0.2.0, while the current version is 0.2.2. It looks like should be _discouraged_ at this point and not _deprecated_ yet. Consider using 'discourage_argument' decorator instead."
        ):

            @rename_argument('aaa', 'bbb', '0.2.0', '0.2.2')
            def early(bbb):
                return bbb

            assert early  # make linters happy

    def test_inverted_versions(self):
        with self.assertRaises(
            AssertionError,
            msg=
            "rename_argument decorator is set to start renaming argument 'aaa' starting at version 0.2.2 and finishing in 0.2.0. It needs to start at a lower version and finish at a higher version."
        ):

            @rename_argument('aaa', 'bbb', '0.2.2', '0.2.0')
            def backwards(bbb):
                return bbb

            assert backwards  # make linters happy


class TestRenameFunction(TestBase):
    def test_rename_function(self):
        def new(bbb):
            return bbb

        for i in ('new', new):

            @rename_function(i, '0.1.0', '0.2.0')
            def old(bbb):
                return bbb

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                assert old(5) == 5
                assert len(w) == 1
                assert issubclass(w[-1].category, DeprecationWarning)
                assert str(
                    w[-1].message
                ) == "'old' is deprecated since version 0.1.0 - it was moved to 'new', please switch to use that. The proxy for the old name is going to be removed in 0.2.0.", str(
                    w[-1].message
                )
