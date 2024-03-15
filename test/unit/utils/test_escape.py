######################################################################
#
# File: test/unit/utils/test_escape.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.utils.escape import escape_control_chars, substitute_control_chars, unprintable_to_hex


def test_unprintable_to_hex():
    cases = [
        (' abc-z', ' abc-z', "' abc-z'", (' abc-z', False)),
        ('a\x7fb', 'a\\x7fb', "'a\\x7fb'", ('a�b', True)),
        ('a\x00b a\x9fb ', 'a\\x00b a\\x9fb ', "'a\\x00b a\\x9fb '", ('a�b a�b ', True)),
        ('a\x7fb\nc', 'a\\x7fb\nc', "'a\\x7fb\nc'", ('a�b\nc', True)),
        ('\x9bT\x9bEtest', '\\x9bT\\x9bEtest', "'\\x9bT\\x9bEtest'", ('�T�Etest', True)),
        (
            '\x1b[32mC\x1b[33mC\x1b[34mI', '\\x1b[32mC\\x1b[33mC\\x1b[34mI',
            "'\\x1b[32mC\\x1b[33mC\\x1b[34mI'", ('�[32mC�[33mC�[34mI', True)
        )
    ]
    for (
        s, expected_unprintable_to_hex, expected_escape_control_chars,
        expected_substitute_control_chars
    ) in cases:
        assert unprintable_to_hex(s) == expected_unprintable_to_hex
        assert escape_control_chars(s) == expected_escape_control_chars
        assert substitute_control_chars(s) == expected_substitute_control_chars
