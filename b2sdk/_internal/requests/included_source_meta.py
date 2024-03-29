######################################################################
#
# File: b2sdk/_internal/requests/included_source_meta.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from b2sdk._internal.included_sources import IncludedSourceMeta, add_included_source

included_source_meta = IncludedSourceMeta(
    'requests', 'Included in a revised form', {
        'NOTICE':
            """Requests
Copyright 2019 Kenneth Reitz

Copyright 2021 Backblaze Inc.
Changes made to the original source:
requests.models.Response.iter_content has been overridden to pass `decode_content=False` argument to `self.raw.stream`
in order to NOT decompress data based on Content-Encoding header"""
    }
)
add_included_source(included_source_meta)
