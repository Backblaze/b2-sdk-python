######################################################################
#
# File: test/unit/utils/test_docs.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest

from b2sdk.raw_api import AbstractRawApi, LifecycleRule
from b2sdk.utils.docs import MissingDocURL, ensure_b2sdk_doc_urls, get_b2sdk_doc_urls


def test_b2sdk_doc_urls():
    @ensure_b2sdk_doc_urls
    class MyCustomClass:
        """
        This is a custom class with `Documentation URL`_.

        .. _Documentation URL: https://example.com
        """


def test_b2sdk_doc_urls__no_urls_error():
    with pytest.raises(MissingDocURL):

        @ensure_b2sdk_doc_urls
        class MyCustomClass:
            pass


@pytest.mark.parametrize(
    'type_,expected', [
        (AbstractRawApi, {}),
        (
            LifecycleRule, {
                'B2 Cloud Storage Lifecycle Rules':
                    'https://www.backblaze.com/docs/cloud-storage-lifecycle-rules',
            }
        ),
    ]
)
def test_get_b2sdk_doc_urls(type_, expected):
    assert get_b2sdk_doc_urls(type_) == expected
