######################################################################
#
# File: test/integration/test_upload.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import io
from typing import Optional

from .fixtures import b2_auth_data  # noqa
from .base import IntegrationTestBase


class TestUnboundStreamUpload(IntegrationTestBase):
    def assert_data_uploaded_via_stream(self, data: bytes, part_size: Optional[int] = None):
        bucket = self.create_bucket()
        stream = io.BytesIO(data)
        file_name = 'unbound_stream'

        bucket.upload_unbound_stream(stream, file_name, recommended_upload_part_size=part_size)

        downloaded_data = io.BytesIO()
        bucket.download_file_by_name(file_name).save(downloaded_data)

        assert downloaded_data.getvalue() == data

    def test_streamed_small_buffer(self):
        # 20kb
        data = b'a small data content' * 1024
        self.assert_data_uploaded_via_stream(data)

    def test_streamed_large_buffer_small_part_size(self):
        # 10mb
        data = b'a large data content' * 512 * 1024
        # 5mb, the smallest allowed part size
        self.assert_data_uploaded_via_stream(data, part_size=5 * 1024 * 1024)
