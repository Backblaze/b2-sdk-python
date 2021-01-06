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
except ImportError:  # ModuleNotFoundError is not available in Python 3.5
    from importlib_metadata import version

VERSION = version('b2sdk')

PYTHON_VERSION = '.'.join(map(str, sys.version_info[:3]))  # something like: 3.9.1

USER_AGENT = 'backblaze-b2/%s python/%s' % (VERSION, PYTHON_VERSION)
