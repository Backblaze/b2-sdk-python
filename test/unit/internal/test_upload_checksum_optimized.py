######################################################################
#
# File: test/unit/internal/test_upload_checksum_optimized.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import subprocess
import sys

import pytest


@pytest.mark.parametrize('kind', ['small', 'part'])
@pytest.mark.parametrize('known_sha1', [True, False])
def test_upload_rejects_bad_server_sha1_under_optimization(kind, known_sha1):
    program = """
from unittest.mock import MagicMock
from b2sdk._internal.exception import ChecksumMismatch
from b2sdk._internal.transfer.outbound.upload_manager import UploadManager
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceBytes
from b2sdk._internal.progress import DoNothingProgressListener
from b2sdk._internal.transfer.outbound.large_file_upload_state import LargeFileUploadState

services = MagicMock()
def upload_file(*args, **kwargs):
    args[6].read()
    return {'contentSha1': 'wrong'}
def upload_part(*args, **kwargs):
    args[4].read()
    return {'contentSha1': 'wrong'}
services.session.upload_file.side_effect = upload_file
services.session.upload_part.side_effect = upload_part
manager = UploadManager(services)
source = UploadSourceBytes(b'hello')
if KNOWN_SHA1:
    source.get_content_sha1()
progress = DoNothingProgressListener()
try:
    if KIND == 'small':
        manager._upload_small_file('bucket', source, 'file', 'text/plain', {}, progress)
    else:
        manager._upload_part('bucket', 'id', source, 1, LargeFileUploadState(progress), {}, None)
except ChecksumMismatch as error:
    if error.expected != source.get_content_sha1() or error.actual != 'wrong':
        raise RuntimeError('checksum error has incorrect values')
else:
    raise RuntimeError('server checksum mismatch was accepted')
"""
    result = subprocess.run(
        [sys.executable, '-O', '-c', f'KIND = {kind!r}\nKNOWN_SHA1 = {known_sha1!r}\n' + program],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
