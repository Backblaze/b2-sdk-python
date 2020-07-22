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

    * :class:`b2sdk.v1.CopySource`
    * :class:`b2sdk.v1.UploadSourceBytes`
    * :class:`b2sdk.v1.UploadSourceLocalFile`
    * :class:`b2sdk.v1.UploadSourceLocalFileRange`
    * :class:`b2sdk.v1.UploadSourceStream`
    * :class:`b2sdk.v1.UploadSourceStreamRange`

    """

    @abstractmethod
    def get_content_length(self):
        """
        Return the number of bytes of data in the file.
        """

    @abstractmethod
    def is_upload(self):
        """ Return if outbound source is an upload source.
        :rtype bool:
        """

    @abstractmethod
    def is_copy(self):
        """ Return if outbound source is a copy source.
        :rtype bool:
        """
