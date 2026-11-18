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


@known_defect('C30', 'typo in .github/SUPPORT.md')
def test_c30_support_file_has_no_known_typo():
    """C30: typo 'Issuew' in .github/SUPPORT.md (codespell did not catch it)."""
    assert 'Issuew' not in _read('.github/SUPPORT.md'), 'typo is still present'


@known_defect('C33', 'Bucket.upload leaves a FIXME about min_part_size semantics')
def test_c33_min_part_size_fixme_resolved():
    """C33: min_part_size docs/code mismatch must not be left as a FIXME in Bucket.upload."""
    assert 'FIXME: Bucket.upload documents wrong logic' not in _read(
        'b2sdk/_internal/bucket.py'
    ), 'the FIXME is still present'


@known_defect('C59', 'the README does not mention the "full" install extra')
def test_c59_readme_mentions_full_extra():
    """C59: README must mention the `full` install extra (tqdm/pydantic support)."""
    assert re.search(r'b2sdk\[full\]', _read('README.md')), 'README does not mention b2sdk[full]'


@known_defect('C4', 'the Quick Start uses a class and an argument name that do not exist')
def test_c4_quick_start_uses_existing_names():
    """C4: Quick Start must not use the non-existent class EncryptionSettings or the wrong `id=` argument."""
    text = _read('doc/source/quick_start.rst')
    assert 'EncryptionSettings(' not in text, 'EncryptionSettings does not exist'
    assert not re.search(r'EncryptionKey\([^)]*\bid=', text), 'EncryptionKey has no id= argument'


@known_defect(
    'C4', 'the development-version install instructions install from PyPI, not the checkout'
)
def test_c4_install_doc_development_version_installs_from_checkout():
    """C4: 'Installing a development version' must install the checkout, not PyPI."""
    section = _read('doc/source/install.rst').split('Installing a development version', 1)[1]
    assert re.search(
        r'pip install (-e )?[.\w/]*\.(\s|$)|pip install -e', section
    ), 'development-version section installs from PyPI'


@known_defect('C36', 'importing b2sdk fails when the package metadata is not installed')
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
        """
    )
    result = subprocess.run(
        [sys.executable, '-c', script], capture_output=True, text=True, cwd=tmp_path
    )
    assert result.returncode == 0, result.stderr[-300:]
