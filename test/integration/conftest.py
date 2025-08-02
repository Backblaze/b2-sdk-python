######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.v3.testing import (  # noqa: F401
    pytest_addoption,
    dont_cleanup_old_buckets,
    bucket_name_prefix,
    general_bucket_name_prefix,
    bucket_manager,
    bucket,
    b2_subfolder,
    set_http_debug,
    b2_auth_data,
    _b2_api,
    b2_api,
)