######################################################################
#
# File: b2sdk/http_constants.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# These constants are needed in different modules, so they are stored in this module, that
# imports nothing, thus avoiding circular imports

FILE_INFO_HEADER_PREFIX = 'X-Bz-Info-'
FILE_INFO_HEADER_PREFIX_LOWER = FILE_INFO_HEADER_PREFIX.lower()

# Standard names for file info entries
SRC_LAST_MODIFIED_MILLIS = 'src_last_modified_millis'

# SHA-1 hash key for large files
LARGE_FILE_SHA1 = 'large_file_sha1'

# Special X-Bz-Content-Sha1 value to verify checksum at the end
HEX_DIGITS_AT_END = 'hex_digits_at_end'

# Identifying SSE_C keys
SSE_C_KEY_ID_FILE_INFO_KEY_NAME = 'sse_c_key_id'
SSE_C_KEY_ID_HEADER = FILE_INFO_HEADER_PREFIX + SSE_C_KEY_ID_FILE_INFO_KEY_NAME

# Default part sizes
MEGABYTE = 1000 * 1000
GIGABYTE = 1000 * MEGABYTE
DEFAULT_MIN_PART_SIZE = 5 * MEGABYTE
DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE = 100 * MEGABYTE
DEFAULT_MAX_PART_SIZE = 5 * GIGABYTE
