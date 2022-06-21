######################################################################
#
# File: b2sdk/requests/included_source_meta.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from b2sdk.included_sources import add_included_source, IncludedSourceMeta

add_included_source(
    IncludedSourceMeta(
        'requests',
        'Included in a revised form, in order to provide a functionality of NOT decompressing encoded content '
        'when downloading', {
            'NOTICE':
                """
Requests
Copyright 2019 Kenneth Reitz

Copyright 2021 Backblaze Inc.
Changes made to the original source:
requests.models.Response.iter_content has been overridden to pass `decode_content=False` argument to `self.raw.stream`
in order to NOT decompress data based on Content-Encoding header
    """
        }
    )
)
