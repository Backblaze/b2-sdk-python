######################################################################
#
# File: b2sdk/sync/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..exception import B2Error


class EnvironmentEncodingError(B2Error):
    """
    Raised when a file name can not be decoded with system encoding
    """

    def __init__(self, filename, encoding):
        """
        :param filename: an encoded file name
        :type filename: str, bytes
        :param encoding: file name encoding
        :type encoding: str
        """
        super(EnvironmentEncodingError, self).__init__()
        self.filename = filename
        self.encoding = encoding

    def __str__(self):
        return """file name %s cannot be decoded with system encoding (%s).
We think this is an environment error which you should workaround by
setting your system encoding properly, for example like this:
export LANG=en_US.UTF-8""" % (
            self.filename,
            self.encoding,
        )
