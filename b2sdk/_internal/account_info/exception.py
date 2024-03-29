######################################################################
#
# File: b2sdk/_internal/account_info/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import ABCMeta

from ..exception import B2Error


class AccountInfoError(B2Error, metaclass=ABCMeta):
    """
    Base class for all account info errors.
    """
    pass


class CorruptAccountInfo(AccountInfoError):
    """
    Raised when an account info file is corrupted.
    """

    def __init__(self, file_name):
        """
        :param file_name: an account info file name
        :type file_name: str
        """
        super().__init__()
        self.file_name = file_name

    def __str__(self):
        return f'Account info file ({self.file_name}) appears corrupted. ' \
               f'Try removing and then re-authorizing the account.'


class MissingAccountData(AccountInfoError):
    """
    Raised when there is no account info data available.
    """

    def __init__(self, key):
        """
        :param key: a key for getting account data
        :type key: str
        """
        super().__init__()
        self.key = key

    def __str__(self):
        return f'Missing account data: {self.key}'
