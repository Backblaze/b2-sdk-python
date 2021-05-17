######################################################################
#
# File: b2sdk/__init__.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# Set default logging handler to avoid "No handler found" warnings.
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())


class UrllibWarningFilter(object):
    def filter(self, record):
        return record.msg != "Connection pool is full, discarding connection: %s"


logging.getLogger('urllib3.connectionpool').addFilter(UrllibWarningFilter())

import b2sdk.version
__version__ = b2sdk.version.VERSION
assert __version__  # PEP-0396

# https://github.com/crsmithdev/arrow/issues/612 - To get rid of the ArrowParseWarning messages in 0.14.3 onward.
try:
    from arrow.factory import ArrowParseWarning
except ImportError:
    pass
else:
    import warnings
    warnings.simplefilter("ignore", ArrowParseWarning)
