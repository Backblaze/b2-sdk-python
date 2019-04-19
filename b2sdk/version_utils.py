######################################################################
#
# File: b2sdk/version_utils.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
from functools import wraps
import inspect
import warnings

from pkg_resources import parse_version
import six

from b2sdk.version import VERSION


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
                self.__class__.__name__,
                self.WHAT,
                self.source,
                self.changed_version,
                self.cutoff_version,
            )
            assert self.current_version < self.cutoff_version, '%s decorator is still used in version %s when old %s name %r was scheduled to be dropped in %s. It is time to remove the mapping.' % (
                self.__class__.__name__,
                self.current_version,
                self.WHAT,
                self.source,
                self.cutoff_version,
            )

    @classmethod
    def _parse_if_not_none(cls, version):
        if version is None:
            return None
        return parse_version(version)

    @abstractmethod
    def __call__(self, func):
        """ the actual implementation of decorator """


class AbstractDeprecator(AbstractVersionDecorator):
    ALTERNATIVE_DECORATOR = NotImplemented

    def __init__(self, target, *args, **kwargs):
        super(AbstractDeprecator, self).__init__(*args, **kwargs)
        assert self.changed_version <= self.current_version, '%s decorator indicates that the replacement of %s %r should take place in the future version %s, while the current version is %s. It looks like should be _discouraged_ at this point and not _deprecated_ yet. Consider using %r decorator instead.' % (
            self.__class__.__name__,
            self.WHAT,
            self.source,
            self.changed_version,
            self.cutoff_version,
            self.ALTERNATIVE_DECORATOR,
        )
        self.target = target


class rename_argument(AbstractDeprecator):
    """
    Changes the argument name to new one if old one is used, warns about deprecation in docs and through a warning

    >>> @rename_argument('aaa', 'bbb', '0.1.0', '0.2.0')
    >>> def easy(bbb):
    >>>     return bbb

    >>> easy(aaa=5)
    'aaa' is a deprecated argument for 'easy' function/method - it was renamed to 'bbb' in version 0.1.0. Support for the old name is going to be dropped in 0.2.0.
    5
    >>>
    """
    WHAT = 'argument'
    ALTERNATIVE_DECORATOR = 'discourage_argument'

    def __init__(self, source, *args, **kwargs):
        self.source = source
        super(rename_argument, self).__init__(*args, **kwargs)

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            message = '%r is not an argument of the decorated function so it cannot be remapped to from a deprecated parameter name' % (
                self.source,
            )
            if six.PY2:
                signature = inspect.getargspec(func)
                assert self.target in signature.args or self.target in signature.varargs, message
            else:
                signature = inspect.getfullargspec(func)
                assert self.target in signature.args or self.target in signature.kwonlyargs, message

            if self.source in kwargs:
                assert self.target not in kwargs, 'both argument names were provided: %r (deprecated) and %r (new)' % (
                    self.source, self.target
                )
                kwargs[self.target] = kwargs[self.source]
                del kwargs[self.source]
                warnings.warn(
                    '%r is a deprecated argument for %r function/method - it was renamed to %r in version %s. Support for the old name is going to be dropped in %s.'
                    % (
                        self.source,
                        func.__name__,
                        self.target,
                        self.changed_version,
                        self.cutoff_version,
                    ),
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
                % (
                    func.__name__,
                    self.changed_version,
                    self.target,
                    self.cutoff_version,
                ),
                DeprecationWarning,
            )
            return func(*args, **kwargs)

        return wrapper


class rename_method(rename_function):
    WHAT = 'method'
    ALTERNATIVE_DECORATOR = 'discourage_method'
