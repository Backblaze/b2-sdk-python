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
            assert (
                self.current_version < self.cutoff_version
            ), f'{self.__class__.__name__} decorator is still used in version {self.current_version} when old {self.WHAT} name {self.source!r} was scheduled to be dropped in {self.cutoff_version}. It is time to remove the mapping.'
        ret = super().__call__(*args, **kwargs)
        assert (
            self.changed_version <= self.current_version
        ), f'{self.__class__.__name__} decorator indicates that the replacement of {self.WHAT} {self.source!r} should take place in the future version {self.changed_version}, while the current version is {self.cutoff_version}. It looks like should be _discouraged_ at this point and not _deprecated_ yet. Consider using {self.ALTERNATIVE_DECORATOR!r} decorator instead.'
        return ret


class rename_argument(_OldAbstractDeprecatorMixin, v3.rename_argument):
    pass


class rename_function(_OldAbstractDeprecatorMixin, v3.rename_function):
    pass
