######################################################################
#
# File: b2sdk/transfer/emerge/write_intent.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class WriteIntent(object):
    """ Wrapper for outbound source that defines destination offset. """

    def __init__(self, outbound_source, destination_offset=0):
        """
        :param b2sdk.v1.OutboundTransferSource outbound_source: data source (remote or local)
        :param int destination_offset: point of start in destination file
        """
        if outbound_source.get_content_length() is None:
            raise ValueError('Cannot wrap outbound source of unknown length')
        self.outbound_source = outbound_source
        self.destination_offset = destination_offset

    def __repr__(self):
        return (
            '<{classname} outbound_source={outbound_source} '
            'destination_offset={destination_offset} id={id}>'
        ).format(
            classname=self.__class__.__name__,
            outbound_source=repr(self.outbound_source),
            destination_offset=self.destination_offset,
            id=id(self),
        )

    @property
    def length(self):
        """ Length of the write intent.

        :rtype: int
        """
        return self.outbound_source.get_content_length()

    @property
    def destination_end_offset(self):
        """ Offset of source end in destination file.

        :rtype: int
        """
        return self.destination_offset + self.length

    def is_copy(self):
        """ States if outbound source is remote source and requires copying.

        :rtype: bool
        """
        return self.outbound_source.is_copy()

    def is_upload(self):
        """ States if outbound source is local source and requires uploading.

        :rtype: bool
        """
        return self.outbound_source.is_upload()

    @classmethod
    def wrap_sources_iterator(cls, outbound_sources_iterator):
        """ Helper that wraps outbound sources iterator with write intents.

        Can be used in cases similar to concatenate to automatically compute destination offsets

        :param: iterator[b2sdk.v1.OutboundTransferSource] outbound_sources_iterator: iterator of outbound sources

        :rtype: generator[b2sdk.v1.WriteIntent]
        """
        current_position = 0
        for outbound_source in outbound_sources_iterator:
            length = outbound_source.get_content_length()
            write_intent = WriteIntent(outbound_source, destination_offset=current_position)
            current_position += length
            yield write_intent
