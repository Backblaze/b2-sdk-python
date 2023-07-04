######################################################################
#
# File: b2sdk/v2/version_utils.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import _v3 as v3


class _OldAbstractDeprecatorMixin:
    def __call__(self, *args, **kwargs):
        if self.cutoff_version:
            assert self.current_version < self.cutoff_version, '%s decorator is still used in version %s when old %s name %r was scheduled to be dropped in %s. It is time to remove the mapping.' % (
                self.__class__.__name__,
                self.current_version,
                self.WHAT,
                self.source,
                self.cutoff_version,
            )
        ret = super().__call__(*args, **kwargs)
        assert self.changed_version <= self.current_version, '%s decorator indicates that the replacement of %s %r should take place in the future version %s, while the current version is %s. It looks like should be _discouraged_ at this point and not _deprecated_ yet. Consider using %r decorator instead.' % (
            self.__class__.__name__,
            self.WHAT,
            self.source,
            self.changed_version,
            self.cutoff_version,
            self.ALTERNATIVE_DECORATOR,
        )
        return ret


class rename_argument(_OldAbstractDeprecatorMixin, v3.rename_argument):
    pass


class rename_function(_OldAbstractDeprecatorMixin, v3.rename_function):
    pass
