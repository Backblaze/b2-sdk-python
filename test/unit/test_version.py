######################################################################
#
# File: test/unit/test_version.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import importlib.util
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from unittest.mock import patch

from packaging.version import Version


def test_version_import_without_package_metadata():
    spec = importlib.util.spec_from_file_location(
        'version_under_test', Path(__file__).resolve().parents[2] / 'b2sdk' / 'version.py'
    )
    module = importlib.util.module_from_spec(spec)
    with patch('importlib.metadata.version', side_effect=PackageNotFoundError('b2sdk')):
        spec.loader.exec_module(module)

    assert module.VERSION
    assert str(Version(module.VERSION)) == module.VERSION
    assert module.USER_AGENT == f'backblaze-b2/{module.VERSION} python/{module.PYTHON_VERSION}'
