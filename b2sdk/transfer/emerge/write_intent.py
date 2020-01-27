from b2sdk.transfer.outbound.copy_source import CopySource
from b2sdk.transfer.outbound.upload_source import AbstractUploadSource


class WriteIntent(object):
    def __init__(self, outbound_source, destination_offset=0):
        if not (isinstance(outbound_source, CopySource) or isinstance(outbound_source, AbstractUploadSource)):
            raise ValueError('Outbound source have to be instance of either of `CopySource` or `AbstractUploadSource`')
        self.outbound_source = outbound_source
        self.destination_offset = destination_offset

    def __repr__(self):
        return ('<{classname} outbound_source={outbound_source} '
                'destination_offset={destination_offset} id={id}>').format(
            classname=self.__class__.__name__,
            outbound_source=repr(self.outbound_source),
            destination_offset=self.destination_offset,
            id=id(self),
        )

    @property
    def length(self):
        return self.outbound_source.get_content_length()

    @property
    def destination_end_offset(self):
        return self.destination_offset + self.length

    def is_copy(self):
        return isinstance(self.outbound_source, CopySource)

    def is_upload(self):
        return isinstance(self.outbound_source, AbstractUploadSource)

    @classmethod
    def wrap_sources_iterator(cls, outbound_sources_iterator):
        current_position = 0
        for outbound_source in outbound_sources_iterator:
            length = outbound_source.get_content_length()
            if length is None:
                raise ValueError('Cannot wrap source of unknown length.')
            write_intent = WriteIntent(outbound_source, destination_offset=current_position)
            current_position += length
            yield write_intent
