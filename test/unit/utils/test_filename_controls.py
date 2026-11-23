######################################################################
#
# File: test/unit/utils/test_filename_controls.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest

from b2sdk._internal.utils import validate_b2_file_name


@pytest.mark.parametrize('codepoint', range(32))
def test_rejects_ascii_control_characters(codepoint):
    with pytest.raises(ValueError):
        validate_b2_file_name(f'folder/name{chr(codepoint)}.txt')


def test_del_is_rejected_and_printable_boundaries_are_allowed():
    with pytest.raises(ValueError):
        validate_b2_file_name('file\x7f.txt')
    validate_b2_file_name('folder/space and tilde~.txt')
    validate_b2_file_name('folder/café.txt')
