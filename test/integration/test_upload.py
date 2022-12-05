######################################################################
#
# File: test/integration/test_download.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import io

from .fixtures import *  # pyflakes: disable
from .base import IntegrationTestBase


class TestUnboundStreamUpload(IntegrationTestBase):
    def test_streamed_buffer(self):
        bucket = self.create_bucket()
        data = b'a large data content' * 100
        stream = io.BytesIO(data)
        file_name = 'unbound_stream'

        bucket.upload_unbound_stream(stream, file_name)

        downloaded_data = io.BytesIO()
        bucket.download_file_by_name(file_name).save(downloaded_data)

        assert downloaded_data.getvalue() == data

