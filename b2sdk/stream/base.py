import io


class ReadOnlyMixin(object):
    def writeable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('Stream with hash cannot be written to')
