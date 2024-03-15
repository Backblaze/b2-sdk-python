######################################################################
#
# File: b2sdk/utils/escape.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import re
import shlex

# skip newline, tab
UNPRINTABLE_PATTERN = re.compile(r'[\x00-\x1f\x7f-\x9f]')


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
    if s:
        return shlex.quote(unprintable_to_hex(s))
    return None
