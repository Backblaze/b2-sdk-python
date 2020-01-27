import six

from abc import ABCMeta, abstractmethod


@six.add_metaclass(ABCMeta)
class OutboundTransferSource(object):
    @abstractmethod
    def get_content_length(self):
        """
        Return the number of bytes of data in the file.
        """
