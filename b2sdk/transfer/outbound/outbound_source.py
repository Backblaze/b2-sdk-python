######################################################################
#
# File: b2sdk/transfer/outbound/outbound_source.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod


class OutboundTransferSource(metaclass=ABCMeta):
    """ Abstract class for defining outbound transfer sources.

    Supported outbound transfer sources are:

    * :class:`b2sdk.v2.CopySource`
    * :class:`b2sdk.v2.UploadSourceBytes`
    * :class:`b2sdk.v2.UploadSourceLocalFile`
    * :class:`b2sdk.v2.UploadSourceLocalFileRange`
    * :class:`b2sdk.v2.UploadSourceStream`
    * :class:`b2sdk.v2.UploadSourceStreamRange`

    """

    @abstractmethod
    def get_content_length(self) -> int:
        """
        Returns the number of bytes of data in the file.
        """

    @abstractmethod
    def is_upload(self) -> bool:
        """
        Returns if outbound source is an upload source.
        """

    @abstractmethod
    def is_copy(self) -> bool:
        """
        Returns if outbound source is a copy source.
        """
