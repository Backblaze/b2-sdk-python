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
import sys

# To avoid confusion between official Backblaze releases of this tool and
# the versions on Github, we use the convention that the third number is
# odd for Github, and even for Backblaze releases.
VERSION = '0.1.4'

PYTHON_VERSION = '.'.join(map(str, sys.version_info[:3]))  # something like: 2.7.11

USER_AGENT = 'backblaze-b2/%s python/%s' % (VERSION, PYTHON_VERSION)


### XXX

from abc import ABCMeta, abstractmethod
import inspect
import warnings

from pkg_resources import parse_version
import six

@six.add_metaclass(ABCMeta)
class AbstractVersionDecorator(object):
    WHAT = NotImplemented  # 'function', 'method', 'class' etc
    def __init__(self, changed_version, cutoff_version=None, reason=''):
        """
        changed_version, cutoff_version and current_version are version strings
        """

        current_version = VERSION  # TODO autodetect by going up the qualname tree and trying getattr(part, '__version__')
        self.current_version = parse_version(current_version)
        self.reason = reason

        self.changed_version = parse_version(changed_version)
        self.cutoff_version = self._parse_if_not_none(cutoff_version)
        if self.cutoff_version is not None:
            assert self.changed_version < self.cutoff_version, '%s decorator is set to start renaming %s %r starting at version %s and finishing in %s. It needs to start at a lower version and finish at a higher version.' % (
                self.__class__.__name__, self.WHAT, self.source, self.changed_version, self.cutoff_version,
            )
            assert self.current_version < self.cutoff_version, '%s decorator is still used in version %s when old %s name %r was scheduled to be dropped in %s. It is time to remove the mapping.' % (
                self.__class__.__name__, self.current_version, self.WHAT, self.source, self.cutoff_version,
            )

    @classmethod
    def _parse_if_not_none(cls, version):
        if version is None:
            return None
        return parse_version(version)

    @abstractmethod
    def __call__(self, func):
        """ the actual implementation of decorator """


class AbstractDiscourager(AbstractVersionDecorator):
    def __call__(self, func):
        # TODO: modify docstring using self.WHAT, self.reason
        return func  # returns unmodified function, only the documentation is modified


class AbstractDeprecator(AbstractVersionDecorator):
    ALTERNATIVE_DECORATOR = NotImplemented
    def __init__(
        self,
        target,
        *args,
        **kwargs
    ):
        super(AbstractDeprecator, self).__init__(*args, **kwargs)
        assert self.changed_version <= self.current_version, '%s decorator indicates that the replacement of %s %r should take place in the future version %s, while the current version is %s. It looks like should be _discouraged_ at this point and not _deprecated_ yet. Consider using %r decorator instead.' % (
            self.__class__.__name__, self.WHAT, self.source, self.changed_version, self.cutoff_version, self.ALTERNATIVE_DECORATOR,
        )
        self.target = target


class discourage_argument(AbstractDiscourager):
    """ Discourages usage of an argument by adding an appropriate note to documentation """
    WHAT = 'argument'


class discourage_function(AbstractDiscourager):
    """ Discourages usage of a function by adding an appropriate note to documentation """
    WHAT = 'function'


class discourage_method(AbstractDiscourager):
    """ Discourages usage of a method by adding an appropriate note to documentation """
    WHAT = 'method'


class rename_argument(AbstractDeprecator):
    """ Changes the argument name to new one if old one is used, warns about deprecation in docs and through a warning """
    WHAT = 'argument'
    ALTERNATIVE_DECORATOR = 'discourage_argument'

    def __init__(
        self,
        source,
        *args,
        **kwargs
    ):
        self.source = source
        super(rename_argument, self).__init__(*args, **kwargs)

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            signature = inspect.getfullargspec(func)
            assert self.target in signature.args or to_ in signature.kwonlyargs, '%r is not an argument of the decorated function so it cannot be remapped to from a deprecated parameter name' % (
                self.source,
            )
            if self.source in kwargs:
                assert self.target not in kwargs, 'both argument names were provided: %r (deprecated) and %r (new)' % (
                    self.source, self.target
                )
                kwargs[self.target] = kwargs[self.source]
                del kwargs[self.source]
                warnings.warn(
                    '%r is a deprecated argument for %r function/method - it was renamed to %r in version %s. Support for the old name is going to be dropped in %s.'
                    % (self.source, func.__name__, self.target, self.changed_version, self.cutoff_version,),
                    DeprecationWarning,
                )
            return func(*args, **kwargs)
        return wrapper


class rename_function(AbstractDeprecator):
    """
    Warns about deprecation in docs and through a DeprecationWarning when used. Use it to decorate a proxy function, like this:

    >>> def new(foobar):
    >>>     return foobar ** 2
    >>> @rename_function(new, '0.1.0', '0.2.0')
    >>> def old(foo, bar):
    >>>     return new(foo + bar)
    >>> old()
    'old' is deprecated since version 0.1.0 - it was moved to 'new', please switch to use that. The proxy for the old name is going to be removed in 0.2.0.
    123
    >>>

    """
    WHAT = 'function'
    ALTERNATIVE_DECORATOR = 'discourage_function'
    def __init__(self, target, *args, **kwargs):
        if callable(target):
            target = target.__name__
        super(rename_function, self).__init__(target, *args, **kwargs)
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                '%r is deprecated since version %s - it was moved to %r, please switch to use that. The proxy for the old name is going to be removed in %s.'
                % (func.__name__, self.changed_version, self.target, self.cutoff_version,), DeprecationWarning,
            )
            return func(*args, **kwargs)
        return wrapper


class rename_method(rename_function):
    WHAT = 'method'
    ALTERNATIVE_DECORATOR = 'discourage_method'


if __name__ == '__main__':
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

    # emit deprecation warning
    assert easy(aaa=5) == 5
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert easy(aaa=5) == 5
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert str(w[-1].message) == "'aaa' is a deprecated argument for 'easy' function/method - it was renamed to 'bbb' in version 0.1.0. Support for the old name is going to be dropped in 0.2.0.", str(w[-1].message)


    #@rename_argument('aaa', 'bbb', '0.1.0', '0.1.2')
    #def late(bbb):
    #    return bbb
    # AssertionError: rename_argument decorator is still used in version 0.1.4 when old argument name 'aaa' was scheduled to be dropped in 0.1.2. It is time to remove the mapping.

    #@rename_argument('aaa', 'bbb', '0.2.0', '0.2.2')
    #def early(bbb):
    #    return bbb
    # AssertionError: rename_argument decorator indicates that the replacement of argument 'aaa' should take place in the future version 0.2.0, while the current version is 0.2.2. It looks like should be _discouraged_ at this point and not _deprecated_ yet. Consider using 'discourage_argument' decorator instead.

    #@rename_argument('aaa', 'bbb', '0.2.2', '0.2.0')
    #def backwards(bbb):
    #    return bbb
    # AssertionError: rename_argument decorator is set to start renaming argument 'aaa' starting at version 0.2.2 and finishing in 0.2.0. It needs to start at a lower version and finish at a higher version.

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
            print(str(w[-1].message))
            assert issubclass(w[-1].category, DeprecationWarning)
            assert str(w[-1].message) == "'old' is deprecated since version 0.1.0 - it was moved to 'new', please switch to use that. The proxy for the old name is going to be removed in 0.2.0.", str(w[-1].message)
