######################################################################
#
# File: b2sdk/account_info/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import (ABCMeta)

import six

from ..exception import B2Error


@six.add_metaclass(ABCMeta)
class AccountInfoError(B2Error):
    """
    Base class for all account info errors
    """
    pass


class CorruptAccountInfo(AccountInfoError):
    """
    Raised when an account info file is corrupted
    """

    def __init__(self, file_name):
        """
        :param file_name: an account info file name
        :type file_name: str
        """
        super(CorruptAccountInfo, self).__init__()
        self.file_name = file_name

    def __str__(self):
        return 'Account info file (%s) appears corrupted.  Try removing and then re-authorizing the account.' % (
            self.file_name,
        )


class MissingAccountData(AccountInfoError):
    """
    Raised when there is no account info data available
    """

    def __init__(self, key):
        """
        :param key: a key for getting account data
        :type key: str
        """
        super(MissingAccountData, self).__init__()
        self.key = key

    def __str__(self):
        return 'Missing account data: %s' % (self.key,)
