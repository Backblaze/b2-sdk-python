######################################################################
#
# File: test/unit/file_lock/test_legal_hold.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from apiver_deps import (
    LegalHold,
)


class TestLegalHold:
    def test_to_dict_repr(self):
        assert LegalHold.ON.to_dict_repr() == 'on'
        assert LegalHold.OFF.to_dict_repr() == 'off'
        assert LegalHold.UNSET.to_dict_repr() is None
        assert LegalHold.UNKNOWN.to_dict_repr() == 'unknown'