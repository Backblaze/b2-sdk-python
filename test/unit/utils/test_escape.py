######################################################################
#
# File: test/unit/utils/test_escape.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest

from b2sdk._internal.utils.escape import (
    escape_control_chars,
    substitute_control_chars,
    unprintable_to_hex,
)


@pytest.mark.parametrize(
    (
        "input_", "expected_unprintable_to_hex", "expected_escape_control_chars",
        "expected_substitute_control_chars"
    ), [
        ('', '', '', ('', False)),
        (' abc-z', ' abc-z', "' abc-z'", (' abc-z', False)),
        ('a\x7fb', 'a\\x7fb', "'a\\x7fb'", ('a�b', True)),
        ('a\x00b a\x9fb ', 'a\\x00b a\\x9fb ', "'a\\x00b a\\x9fb '", ('a�b a�b ', True)),
        ('a\x7fb\nc', 'a\\x7fb\nc', "'a\\x7fb\nc'", ('a�b\nc', True)),
        ('\x9bT\x9bEtest', '\\x9bT\\x9bEtest', "'\\x9bT\\x9bEtest'", ('�T�Etest', True)),
        (
            '\x1b[32mC\x1b[33mC\x1b[34mI', '\\x1b[32mC\\x1b[33mC\\x1b[34mI',
            "'\\x1b[32mC\\x1b[33mC\\x1b[34mI'", ('�[32mC�[33mC�[34mI', True)
        ),
    ]
)
def test_unprintable_to_hex(
    input_, expected_unprintable_to_hex, expected_escape_control_chars,
    expected_substitute_control_chars
):
    assert unprintable_to_hex(input_) == expected_unprintable_to_hex
    assert escape_control_chars(input_) == expected_escape_control_chars
    assert substitute_control_chars(input_) == expected_substitute_control_chars


def test_unprintable_to_hex__none():
    """
    Test that unprintable_to_hex handles None.

    This was unintentionally supported and is only kept for compatibility.
    """
    assert unprintable_to_hex(None) is None  # type: ignore


def test_escape_control_chars__none():
    """
    Test that escape_control_chars handles None.

    This was unintentionally supported and is only kept for compatibility.
    """
    assert escape_control_chars(None) is None  # type: ignore


def test_substitute_control_chars__none():
    with pytest.raises(TypeError):
        substitute_control_chars(None)  # type: ignore
