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

from test.known_defect import DefectStillPresent, expect, known_defect

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


# an EncryptionKey(...) call whose arguments contain `id=`; quoted strings are skipped as a whole,
# so a ")" inside the secret does not end the argument list early
ENCRYPTION_KEY_WITH_ID = re.compile(r"""EncryptionKey\((?:[^()'"]|'[^']*'|"[^"]*")*?\bid=""")
# installs the local checkout (`pip install .` or `pip install -e .`), not a package index or a URL
PIP_INSTALL_CHECKOUT = re.compile(r'pip install (?:-e\s+)?\.(?:\[[^\]]*\])?(?=\s|$)')


@known_defect('C4', 'the Quick Start uses a class that does not exist', raises=AssertionError)
def test_c4_quick_start_does_not_use_encryption_settings_class():
    """C4: Quick Start must not use the non-existent class EncryptionSettings."""
    assert 'EncryptionSettings(' not in _read(
        'doc/source/quick_start.rst'
    ), 'EncryptionSettings does not exist'


@known_defect(
    'C4',
    'the Quick Start uses an EncryptionKey argument that does not exist',
    raises=AssertionError,
)
def test_c4_quick_start_uses_key_id_argument_of_encryption_key():
    """C4: Quick Start must call EncryptionKey with `key_id=`, which is the argument it has."""
    assert not ENCRYPTION_KEY_WITH_ID.search(
        _read('doc/source/quick_start.rst')
    ), 'EncryptionKey has no id= argument'


@known_defect(
    'C4',
    'the development-version install instructions install from PyPI, not the checkout',
    raises=AssertionError,
)
def test_c4_install_doc_development_version_installs_from_checkout():
    """C4: 'Installing a development version' must install the checkout, not PyPI."""
    section = _read('doc/source/install.rst').split('Installing a development version', 1)[1]
    assert PIP_INSTALL_CHECKOUT.search(section), 'development-version section installs from PyPI'


@known_defect(
    'C36',
    'importing b2sdk fails when the package metadata is not installed',
    raises=DefectStillPresent,
)
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
    # only the missing-metadata error is the defect; any other failure is a real failure
    expect('PackageNotFoundError' not in result.stderr, result.stderr[-300:])
    assert result.returncode == 0, result.stderr[-300:]
