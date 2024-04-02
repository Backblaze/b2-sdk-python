######################################################################
#
# File: b2sdk/_internal/utils/escape.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import re
import shlex

# skip newline, tab
UNPRINTABLE_PATTERN = re.compile(r'[\x00-\x08\x0e-\x1f\x7f-\x9f]')


def unprintable_to_hex(s):
    """
    Replace unprintable chars in string with a hex representation.

    :param string: an arbitrary string, possibly with unprintable characters.
    :return: the string, with unprintable characters changed to hex (e.g., "\x07")

    """

    def hexify(match):
        return fr'\x{ord(match.group()):02x}'

    if s:
        return UNPRINTABLE_PATTERN.sub(hexify, s)
    return None


def escape_control_chars(s):
    """
    Replace unprintable chars in string with a hex representation AND shell quotes the string.

    :param string: an arbitrary string, possibly with unprintable characters.
    :return: the string, with unprintable characters changed to hex (e.g., "\x07")

    """
    if s:
        return shlex.quote(unprintable_to_hex(s))
    return None


def substitute_control_chars(s):
    """
    Replace unprintable chars in string with � unicode char

    :param string: an arbitrary string, possibly with unprintable characters.
    :return: tuple of the string with � replacements made and boolean indicated if chars were replaced

    """
    match_result = UNPRINTABLE_PATTERN.search(s)
    s = UNPRINTABLE_PATTERN.sub('�', s)
    return (s, match_result is not None)
