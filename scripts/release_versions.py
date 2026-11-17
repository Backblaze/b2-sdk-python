######################################################################
#
# File: scripts/release_versions.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""The release interface supports stable and Python-compatible a/b/rc versions."""

VERSION_PATTERN = (
    r'(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)' r'(?:-(?:a|b|rc)\.(?:0|[1-9]\d*))?'
)
