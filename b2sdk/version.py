######################################################################
#
# File: b2sdk/version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from functools import wraps
import inspect
import sys
import warnings

import six

# To avoid confusion between official Backblaze releases of this tool and
# the versions on Github, we use the convention that the third number is
# odd for Github, and even for Backblaze releases.
VERSION = '0.1.4'

PYTHON_VERSION = '.'.join(map(str, sys.version_info[:3]))  # something like: 2.7.11

USER_AGENT = 'backblaze-b2/%s python/%s' % (VERSION, PYTHON_VERSION)


class Deprecat:
    def _get_verified_versions(self, tuple_, subject, from_=None, allow_future=False):
        """
        subject may be "function", "method", "argument", "class" etc
        returns None if replacement is going to happen in the future, or a 3-tuple with replacemnt specification:
            * name to replace to
            * version where the change was introduced
            * version where the support for the old name is going to be dropped
        """
        to_, version_changed, version_dropped = tuple_
        assert len(version_changed) == len(version_dropped) == len(
            VERSION
        ) == 5  # TODO the comparator used below is too naiive for the provided version number
        assert version_changed < version_dropped, '%s decorator is set to start renaming %s %r starting at version %s and finishing in %s. It needs to start at a lower version and finish at a higher version.' % (
            self.__class__.__name__, subject, from_, version_changed, version_dropped
        )
        assert VERSION < version_dropped, '%s decorator is still used in version %s when old %s name %r was scheduled to be dropped in %s. It is time to remove that mapping.' % (
            self.__class__.__name__, VERSION, subject, from_, version_dropped
        )
        if not allow_future:
            assert version_changed <= VERSION, '%s decorator indicates that the replacement of %s %s should take place in the future version %s, while the current version is %s' % (
                self.__class__.__name__, subject, from_, version_changed, VERSION
            )
        if version_changed >= VERSION:
            return None
        return to_, version_changed, version_dropped

    def __init__(
        self,
        replacements=None,
        rename=None,
        allow_future_replacements=False,
        allow_future_rename=False,
    ):
        self.rename = rename
        if self.rename is not None:
            _ = self._get_verified_versions(
                self.rename,
                'function',
                allow_future=allow_future_rename,
            )

        if replacements is None:
            replacements = {}

        to_remove = set()
        self.replacements = {}
        for from_, tuple_ in six.iteritems(replacements):
            parsed = self._get_verified_versions(
                tuple_,
                'argument',
                from_=from_,
                allow_future=allow_future_replacements,
            )
            if parsed is not None:
                self.replacements[from_] = parsed

    def __call__(self, callable):
        @wraps(callable)
        def wrapper(*args, **kwargs):
            if self.rename is not None:
                to_, version_changed, version_dropped = self.rename
                warnings.warn(
                    '%r is deprecated since version %s - it was moved to %r, please switch to use that. The proxy for the old name is going to be removed in %s.'
                    % (callable.__name__, version_changed, to_, version_dropped), DeprecationWarning
                )
            signature = inspect.getfullargspec(callable)
            for from_, (to_, version_changed, version_dropped) in six.iteritems(self.replacements):
                assert to_ in signature.args or to_ in signature.kwonlyargs, '%r is not an argument of the decorated function so it cannot be remapped to from a deprecated parameter name' % (
                    from_,
                )
                if from_ in kwargs:
                    assert to_ not in kwargs, 'both argument names were provided: %r (deprecated) and %r (new)' % (
                        from_, to_
                    )
                    kwargs[to_] = kwargs[from_]
                    del kwargs[from_]
                    warnings.warn(
                        '%r is a deprecated argument for %r function/method - it was renamed to %r in version %s. Support for the old name is going to be dropped in %s'
                        % (from_, callable.__name__, to_, version_changed, version_dropped),
                        DeprecationWarning,
                    )
            return callable(*args, **kwargs)

        return wrapper


if __name__ == '__main__':
    # yapf: disable
    @Deprecat(
        replacements={
            'aaa': ('bbb', '0.1.0', '0.2.0'),
        },
    )
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

    # emit deprecation warning
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert easy(aaa=5) == 5
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert str(w[-1].message) == "'aaa' is a deprecated argument for 'easy' function/method - it was renamed to 'bbb' in version 0.1.0. Support for the old name is going to be dropped in 0.2.0"


    #@Deprecat(
    #    replacements={
    #        'aaa': ('bbb', '0.1.0', '0.1.2'),
    #    },
    #)
    #def late(bbb):
    #    return bbb
    # AssertionError: Deprecat decorator is still used in version 0.1.4 when old argument name 'aaa' was scheduled to be dropped in 0.1.2. It is time to remove that mapping.

    #@Deprecat(
    #    replacements={
    #        'aaa': ('bbb', '0.2.0', '0.2.2'),
    #    },
    #)
    #def early(bbb):
    #    return bbb
    # AssertionError: Deprecat decorator indicates that the replacement of aaa should take place in the future version 0.2.0, while the current version is 0.1.4

    @Deprecat(
        replacements={
            'aaa': ('bbb', '0.2.0', '0.2.2'),
        },
        allow_future_replacements=True
    )
    def early(bbb):
        return bbb
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert early(5) == 5
        assert early(bbb=5) == 5
        #assert early(aaa=5) == 5
        # TypeError: early() got an unexpected keyword argument 'aaa'
        assert len(w) == 0

    #@Deprecat(
    #    replacements={
    #        'aaa': ('bbb', '0.2.2', '0.2.0'),
    #    },
    #)
    #def backwards(bbb):
    #    return bbb
    # AssertionError: Deprecat decorator is set to start renaming argument 'aaa' starting at version 0.2.2 and finishing in 0.2.0. It needs to start at a lower version and finish at a higher version.

    @Deprecat(
        rename=('new', '0.1.0', '0.2.0'),
    )
    def old(bbb):
        return bbb
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert old(5) == 5
        assert len(w) == 1
        print(str(w[-1].message))
        assert issubclass(w[-1].category, DeprecationWarning)
        assert str(w[-1].message) == "'old' is deprecated since version 0.1.0 - it was moved to 'new', please switch to use that. The proxy for the old name is going to be removed in 0.2.0."

    # yapf: enable
