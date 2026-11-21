######################################################################
#
# File: test/static/test_regression_repo_checks.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Static regression checks of repository files (docs, changelog, packaging, sources).

They read repository files only and are skipped when the repository files are not available.

Tests marked with ``@known_defect`` describe a defect that is still present: they assert the correct
behaviour and are strict expected failures, so the default run stays green and fails with
"XPASS(strict)" once the defect is fixed (then remove the decorator). To see them fail, run
``pytest --runxfail -m known_defect`` (see ``test/known_defect.py``). The ``C<n>`` identifiers are
short labels that also appear in the test names and decorators.
"""

from __future__ import annotations

import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from test.known_defect import known_defect

ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    path = ROOT / rel
    if not path.is_file():
        pytest.skip(f'{rel} is not available (not a full repository checkout)')
    return path.read_text(encoding='utf-8')


@known_defect('C30', 'typo in .github/SUPPORT.md', raises=AssertionError)
def test_c30_support_file_has_no_known_typo():
    """C30: typo 'Issuew' in .github/SUPPORT.md (codespell did not catch it)."""
    assert 'Issuew' not in _read('.github/SUPPORT.md'), 'typo is still present'


@known_defect('C59', 'the README does not mention the "full" install extra', raises=AssertionError)
def test_c59_readme_mentions_full_extra():
    """C59: README must mention the `full` install extra (tqdm/pydantic support)."""
    assert re.search(r'b2sdk\[full\]', _read('README.md')), 'README does not mention b2sdk[full]'


def test_c36_version_import_works_when_package_metadata_is_missing(tmp_path):
    """C36: importing a vendored/uninstalled copy of b2sdk must not fail for missing dist metadata."""
    script = textwrap.dedent(
        f"""
        import importlib.metadata as m, sys
        orig = m.version
        def fake(name):
            if name == 'b2sdk':
                raise m.PackageNotFoundError(name)
            return orig(name)
        m.version = fake
        sys.path.insert(0, {str(ROOT)!r})
        import b2sdk.version
        assert b2sdk.__version__ == b2sdk.version.VERSION
        assert b2sdk.version.VERSION
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script], capture_output=True, text=True, cwd=tmp_path
    )
    assert result.returncode == 0, result.stderr[-300:]
