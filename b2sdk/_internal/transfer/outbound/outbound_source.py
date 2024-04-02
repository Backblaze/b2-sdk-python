######################################################################
#
# File: b2sdk/_internal/transfer/outbound/outbound_source.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import ABCMeta, abstractmethod

from b2sdk._internal.utils import Sha1HexDigest


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
    def get_content_sha1(self) -> Sha1HexDigest | None:
        """
        Return a 40-character string containing the hex SHA1 checksum, which can be used as the `large_file_sha1` entry.

        This method is only used if a large file is constructed from only a single source.  If that source's hash is known,
        the result file's SHA1 checksum will be the same and can be copied.

        If the source's sha1 is unknown and can't be calculated, `None` is returned.
        """

    @abstractmethod
    def is_upload(self) -> bool:
        """
        Returns True if outbound source is an upload source.
        """

    @abstractmethod
    def is_copy(self) -> bool:
        """
        Returns True if outbound source is a copy source.
        """
