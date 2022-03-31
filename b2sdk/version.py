######################################################################
#
# File: b2sdk/version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys

try:
    from importlib.metadata import version
except ModuleNotFoundError:
    from importlib_metadata import version  # for python 3.7

VERSION = version('b2sdk')

PYTHON_VERSION = '.'.join(map(str, sys.version_info[:3]))  # something like: 3.9.1

USER_AGENT = 'backblaze-b2/%s python/%s' % (VERSION, PYTHON_VERSION)
