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
        assert 'on' == LegalHold.ON.to_dict_repr()
        assert 'off' == LegalHold.OFF.to_dict_repr()
        assert 'off' == LegalHold.UNSET.to_dict_repr()
        assert 'unknown' == LegalHold.UNKNOWN.to_dict_repr()