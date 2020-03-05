import io


class ReadOnlyStreamMixin(object):
    def writeable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('Cannot accept a write to a read-only stream')
