######################################################################
#
# File: b2sdk/sync/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..exception import B2Error, B2SimpleError


class EnvironmentEncodingError(B2Error):
    """
    Raised when a file name can not be decoded with system encoding.
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


class InvalidArgument(B2Error):
    """
    Raised when one or more arguments are invalid
    """

    def __init__(self, field_name, message):
        """

        :param field_name:
        :param message:
        :return:
        """
        super(InvalidArgument, self).__init__()
        self.field_name = field_name
        self.message = message

    def __str__(self):
        return "%s %s" % (self.field_name, self.message)


class IncompleteSync(B2SimpleError):
    pass
