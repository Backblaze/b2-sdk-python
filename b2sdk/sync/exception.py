######################################################################
#
# File: b2sdk/sync/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from contextlib import contextmanager
from typing import Iterator, Type

from ..exception import B2Error, B2SimpleError


class EnvironmentEncodingError(B2Error):
    """
    Raised when a file name can not be decoded with system encoding.
    """

    def __init__(self, filename, encoding):
        """
        :param filename: an encoded file name
        :type filename: str, bytes
        :param str encoding: file name encoding
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

    def __init__(self, parameter_name, message):
        """
        :param parameter_name: name of the function argument
        :param message: brief explanation of misconfiguration
        """
        super(InvalidArgument, self).__init__()
        self.parameter_name = parameter_name
        self.message = message

    def __str__(self):
        return "%s %s" % (self.parameter_name, self.message)


class IncompleteSync(B2SimpleError):
    pass


class UnSyncableFilename(B2Error):
    """
    Raised when a filename is not supported by the sync operation
    """

    def __init__(self, message, filename):
        """
        :param message: brief explanation of why the filename was not supported
        :param filename: name of the file which is not supported
        """
        super(UnSyncableFilename, self).__init__()
        self.filename = filename
        self.message = message

    def __str__(self):
        return "%s: %s" % (self.message, self.filename)


@contextmanager
def check_invalid_argument(parameter_name: str, message: str,
                           *exceptions: Type[Exception]) -> Iterator[None]:
    """Raise `InvalidArgument` in case of one of given exception was thrown."""
    try:
        yield
    except exceptions as exc:
        if not message:
            message = str(exc)
        raise InvalidArgument(parameter_name, message) from exc


class BaseDirectoryError(B2SimpleError):
    def __init__(self, path):
        self.path = path
        super().__init__(path)


class EmptyDirectory(BaseDirectoryError):
    pass


class UnableToCreateDirectory(BaseDirectoryError):
    pass


class NotADirectory(BaseDirectoryError):
    pass
