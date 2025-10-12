######################################################################
#
# File: test/unit/test_upload_503_token_refresh.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Test to verify that upload tokens are refreshed when a 503 error is returned.
This test simulates the scenario where a server returns a 503 Service Unavailable
error during file upload and verifies that the SDK properly requests new upload
tokens for subsequent retry attempts.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from b2sdk._internal.exception import ServiceError
from b2sdk.v3 import RawSimulator
from b2sdk.v3 import B2Api, B2HttpApiConfig, DummyCache, StubAccountInfo

from .test_base import TestBase


class TestUpload503TokenRefresh(TestBase):
    """Test that 503 errors trigger upload token refresh using RawSimulator"""

    def setUp(self):
        self.bucket_name = 'test-bucket'
        self.account_info = StubAccountInfo()
        self.api = B2Api(
            self.account_info,
            cache=DummyCache(),
            api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
        )
        self.simulator = self.api.session.raw_api
        (self.account_id, self.master_key) = self.simulator.create_account()
        self.api.authorize_account(
            application_key_id=self.account_id,
            application_key=self.master_key,
            realm='production',
        )
        self.bucket = self.api.create_bucket(self.bucket_name, 'allPublic')

    def test_upload_503_triggers_token_refresh(self):
        """
        Test that when a 503 error occurs during upload, the SDK:
        1. Retries the upload
        2. Uses a different upload token for the retry
        """
        # Track upload URLs/tokens used during upload attempts
        upload_urls_used = []

        # Wrap the upload_file method to track upload URLs
        original_upload_file = self.simulator.upload_file

        def tracked_upload_file(upload_url, *args, **kwargs):
            upload_urls_used.append(upload_url)
            return original_upload_file(upload_url, *args, **kwargs)

        # Inject a 503 error for the first upload attempt
        self.simulator.set_upload_errors(
            [ServiceError('503 service_unavailable Service Unavailable')]
        )

        # Patch upload_file to track URLs
        with patch.object(self.simulator, 'upload_file', side_effect=tracked_upload_file):
            # Perform upload - should fail once with 503, then retry and succeed
            data = b'test data for 503 error scenario'
            file_name = 'test_503.txt'

            self.bucket.upload_bytes(data, file_name)

        # Verify the upload succeeded
        file_info = self.bucket.get_file_info_by_name(file_name)
        assert file_info is not None

        # Verify that at least 2 upload attempts were made
        assert (
            len(upload_urls_used) >= 2
        ), f'Expected at least 2 upload attempts, but got {len(upload_urls_used)}'

        # Extract auth tokens from the URLs
        # URL format: https://upload.example.com/bucket_id/upload_id/auth_token
        first_url = upload_urls_used[0]
        second_url = upload_urls_used[1]

        first_auth_token = first_url.split('/')[-1]
        second_auth_token = second_url.split('/')[-1]

        print('\n✓ Upload token refresh test results:')
        print(f'  First URL:   {first_url}')
        print(f'  Second URL:  {second_url}')
        print(f'  First auth token:  {first_auth_token}')
        print(f'  Second auth token: {second_auth_token}')
        print(f'  Total upload attempts: {len(upload_urls_used)}')

        # Verify that auth tokens are different after a 503 error.
        # This confirms that the SDK properly clears cached upload tokens
        # and requests fresh ones when a 503 Service Error occurs.
        assert first_auth_token != second_auth_token, (
            f'BUG: Auth tokens are the same after 503 error!\n'
            f'Expected different auth tokens, but got:\n'
            f'  First:  {first_auth_token}\n'
            f'  Second: {second_auth_token}\n'
            f'This indicates the SDK is cycling through pre-fetched upload URLs\n'
            f'instead of requesting fresh upload tokens after a 503 error.'
        )
