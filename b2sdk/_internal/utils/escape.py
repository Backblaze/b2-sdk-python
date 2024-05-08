######################################################################
#
# File: b2sdk/_internal/utils/escape.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import annotations

import re
import shlex

# skip newline, tab
UNPRINTABLE_PATTERN = re.compile(r'[\x00-\x08\x0e-\x1f\x7f-\x9f]')


def unprintable_to_hex(s: str) -> str:
    """
    Replace unprintable chars in string with a hex representation.

    :param s: an arbitrary string, possibly with unprintable characters.
    :return: the string, with unprintable characters changed to hex (e.g., "\x07")
    """

    def hexify(match):
        return rf"\x{ord(match.group()):02x}"

    if s:
        return UNPRINTABLE_PATTERN.sub(hexify, s)
    return s


def escape_control_chars(s: str) -> str:
    """
    Replace unprintable chars in string with a hex representation AND shell quotes the string.

    :param s: an arbitrary string, possibly with unprintable characters.
    :return: the string, with unprintable characters changed to hex (e.g., "\x07")
    """
    if s:
        return shlex.quote(unprintable_to_hex(s))
    return s


def substitute_control_chars(s: str) -> tuple[str, bool]:
    """
    Replace unprintable chars in string with � unicode char

    :param s: an arbitrary string, possibly with unprintable characters.
    :return: tuple of the string with � replacements made and boolean indicated if chars were replaced
    """
    new_value = UNPRINTABLE_PATTERN.sub("�", s)
    return new_value, new_value != s
