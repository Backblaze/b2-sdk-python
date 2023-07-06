######################################################################
#
# File: b2sdk/version_utils.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import inspect
import warnings
from abc import ABCMeta, abstractmethod
from functools import wraps

from pkg_resources import parse_version

from b2sdk.version import VERSION


class AbstractVersionDecorator(metaclass=ABCMeta):
    WHAT = NotImplemented  # 'function', 'method', 'class' etc

    def __init__(self, changed_version, cutoff_version=None, reason='', current_version=None):
        """
        Changed_version, cutoff_version and current_version are version strings.
        """
        if current_version is None:  # this is for tests only
            current_version = VERSION  # TODO autodetect by going up the qualname tree and trying getattr(part, '__version__')
        self.current_version = parse_version(current_version)  #: current version
        self.reason = reason

        self.changed_version = self._parse_if_not_none(
            changed_version
        )  #: version in which the decorator was added
        self.cutoff_version = self._parse_if_not_none(
            cutoff_version
        )  #: version in which the decorator (and something?) shall be removed

    @classmethod
    def _parse_if_not_none(cls, version):
        if version is None:
            return None
        return parse_version(version)

    @abstractmethod
    def __call__(self, func):
        """
        The actual implementation of decorator. Needs self.source to be set before it's called.
        """
        if self.cutoff_version and self.changed_version:
            assert self.changed_version < self.cutoff_version, '{} decorator is set to start renaming {} {!r} starting at version {} and finishing in {}. It needs to start at a lower version and finish at a higher version.'.format(
                self.__class__.__name__,
                self.WHAT,
                self.source,
                self.changed_version,
                self.cutoff_version,
            )


class AbstractDeprecator(AbstractVersionDecorator):
    ALTERNATIVE_DECORATOR = NotImplemented

    def __init__(self, target, *args, **kwargs):
        self.target = target
        super().__init__(*args, **kwargs)


class rename_argument(AbstractDeprecator):
    """
    Change the argument name to new one if old one is used, warns about deprecation in docs and through a warning.

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
        super().__init__(*args, **kwargs)

    def __call__(self, func):
        super().__call__(func)
        signature = inspect.signature(func)
        has_target_arg = self.target in signature.parameters or any(
            p.kind == p.VAR_KEYWORD for p in signature.parameters.values()
        )
        assert has_target_arg, f'{self.target!r} is not an argument of the decorated function so it cannot be remapped to from a deprecated parameter name'

        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.source in kwargs:
                assert self.target not in kwargs, 'both argument names were provided: {!r} (deprecated) and {!r} (new)'.format(
                    self.source, self.target
                )
                kwargs[self.target] = kwargs[self.source]
                del kwargs[self.source]
                info = f'{self.source!r} is a deprecated argument for {func.__name__!r} function/method - it was renamed to {self.target!r}'
                if self.changed_version:
                    info += f' in version {self.changed_version}'
                if self.cutoff_version:
                    info += f'. Support for the old name is going to be dropped in {self.cutoff_version}'

                warnings.warn(
                    f"{info}.",
                    DeprecationWarning,
                )
            return func(*args, **kwargs)

        return wrapper


class rename_function(AbstractDeprecator):
    """
    Warn about deprecation in docs and through a DeprecationWarning when used.  Use it to decorate a proxy function, like this:

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
        super().__init__(target, *args, **kwargs)

    def __call__(self, func):
        self.source = func.__name__
        super().__call__(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                '{!r} is deprecated since version {} - it was moved to {!r}, please switch to use that. The proxy for the old name is going to be removed in {}.'
                .format(
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
