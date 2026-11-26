######################################################################
#
# File: test/unit/test_documentation_examples.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Validate the Python examples published in the README and the Sphinx docs.

The examples talk to a live B2 account, so they cannot be executed here. What can drift silently
(and what users copy first) are the `import` statements: a renamed or removed public name leaves
the documentation importing something that no longer exists. This module extracts every Python
`code-block` from the docs and checks that each `import` statement in it resolves against the
installed `b2sdk` package.
"""

from __future__ import annotations

import ast
import importlib
import pathlib
import re
import textwrap

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DOC_SOURCE = REPO_ROOT / 'doc' / 'source'

_CODE_BLOCK_RE = re.compile(r'^(?P<indent>[ ]*)\.\. code-block:: python[ ]*$')
_MARKDOWN_FENCE_RE = re.compile(r'^```(?:python|py)[ ]*$')


def extract_python_blocks(text: str) -> list[str]:
    """Return the body of each Python code block (reST `code-block` or Markdown fence) in `text`."""
    lines = text.splitlines()
    blocks: list[str] = []
    i = 0
    while i < len(lines):
        rst = _CODE_BLOCK_RE.match(lines[i])
        if rst:
            base_indent = len(rst.group('indent'))
            i += 1
            body: list[str] = []
            while i < len(lines):
                line = lines[i]
                if line.strip() and len(line) - len(line.lstrip()) <= base_indent:
                    break
                body.append(line)
                i += 1
            blocks.append(textwrap.dedent('\n'.join(body)).strip('\n'))
        elif _MARKDOWN_FENCE_RE.match(lines[i]):
            i += 1
            body = []
            while i < len(lines) and not lines[i].startswith('```'):
                body.append(lines[i])
                i += 1
            blocks.append('\n'.join(body))
            i += 1
        else:
            i += 1
    return blocks


def import_statements(block: str) -> list[str]:
    """
    Return the `import` statements found in a block.

    Blocks are written either as plain Python or as an interactive session (`>>> ` prompts);
    for the latter only the prompt lines are code, the rest is sample output.
    """
    interactive = any(line.lstrip().startswith('>>> ') for line in block.splitlines())
    statements = []
    for line in block.splitlines():
        stripped = line.strip()
        if interactive:
            if not stripped.startswith('>>> '):
                continue
            stripped = stripped[4:].strip()
        if stripped.startswith('import ') or (
            stripped.startswith('from ') and ' import ' in stripped
        ):
            statements.append(stripped)
    return statements


def assert_import_resolves(statement: str) -> None:
    """Execute-free check that an `import` statement names things that really exist."""
    node = ast.parse(statement).body[0]
    if isinstance(node, ast.Import):
        for alias in node.names:
            importlib.import_module(alias.name)
    elif isinstance(node, ast.ImportFrom):
        assert node.level == 0, f'relative import in documentation: {statement!r}'
        module = importlib.import_module(node.module)
        for alias in node.names:
            if alias.name == '*':
                continue
            assert hasattr(
                module, alias.name
            ), f'{statement!r}: module {node.module!r} has no public name {alias.name!r}'
    else:  # pragma: no cover - guarded by import_statements
        raise AssertionError(f'not an import statement: {statement!r}')


def documentation_files() -> list[pathlib.Path]:
    return [REPO_ROOT / 'README.md', *sorted(DOC_SOURCE.glob('*.rst'))]


class TestExtraction:
    def test_rst_block_stops_at_dedent(self):
        text = (
            'Title\n=====\n\n.. code-block:: python\n\n    >>> import time\n\n'
            '    >>> time.sleep(0)\n\nProse after the block.\n'
        )
        assert extract_python_blocks(text) == ['>>> import time\n\n>>> time.sleep(0)']

    def test_markdown_fence(self):
        text = 'Intro\n\n```python\nimport os\n```\n\n```sh\nls\n```\n'
        assert extract_python_blocks(text) == ['import os']

    def test_non_python_blocks_are_ignored(self):
        assert extract_python_blocks('.. code-block:: sh\n\n    import fake\n') == []

    def test_import_statements_from_interactive_block(self):
        block = '>>> from b2sdk.v3 import B2Api\n>>> b2_api = B2Api()\nfrom not_code import x'
        assert import_statements(block) == ['from b2sdk.v3 import B2Api']

    def test_import_statements_from_plain_block(self):
        block = 'import os\nfrom os import path\nvalue = 1  # import not_a_statement'
        assert import_statements(block) == ['import os', 'from os import path']


class TestImportResolution:
    def test_detects_missing_name(self):
        with pytest.raises(AssertionError, match='no public name'):
            assert_import_resolves('from b2sdk.v3 import ThisNameDoesNotExist')

    def test_detects_missing_module(self):
        with pytest.raises(ImportError):
            assert_import_resolves('import b2sdk.v3.no_such_module')

    def test_accepts_real_names(self):
        assert_import_resolves('from b2sdk.v3 import B2Api, InMemoryAccountInfo')
        assert_import_resolves('from b2sdk.v3 import *')


def _documented_imports() -> list:
    params = []
    for path in documentation_files():
        for block in extract_python_blocks(path.read_text(encoding='utf-8')):
            for statement in import_statements(block):
                params.append(
                    pytest.param(
                        statement, id=f'{path.relative_to(REPO_ROOT).as_posix()}::{statement}'
                    )
                )
    return params


def test_documentation_contains_examples_to_validate():
    # Guard against the extraction silently matching nothing (e.g. after a docs reformat).
    assert len(_documented_imports()) >= 10


@pytest.mark.parametrize('statement', _documented_imports())
def test_documented_import_resolves_against_public_api(statement):
    assert_import_resolves(statement)
