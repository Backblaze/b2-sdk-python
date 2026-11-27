######################################################################
#
# File: test/static/test_agent_instructions.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NOX_SESSION_RE = re.compile(r'\bnox\b(?:\s+-\S+)*\s+-s\s+(\w+)')
LINK_RE = re.compile(r'\]\(([^)\s]+)\)')
TICK_RE = re.compile(
    r'`((?:(?:b2sdk|test|doc|docs|changelog\.d|\.sdkharness|\.github|\.claude|_internal)/[^`\s]*)'
    r'|(?:noxfile\.py|[A-Za-z][\w.-]*\.(?:md|toml|lock|txt)))`'
)
PLACEHOLDER_RE = re.compile(r'v[Nn]|[<>*{]')


def noxfile_sessions(noxfile):
    """Read session names statically: noxfile.py must not be imported."""
    names = set()
    for node in ast.walk(ast.parse(noxfile.read_text(encoding='utf-8'))):
        if not isinstance(node, ast.FunctionDef):
            continue
        for deco in node.decorator_list:
            func = deco.func if isinstance(deco, ast.Call) else deco
            if not (isinstance(func, ast.Attribute) and func.attr == 'session'):
                continue
            name = node.name
            for kw in deco.keywords if isinstance(deco, ast.Call) else []:
                if kw.arg == 'name' and isinstance(kw.value, ast.Constant):
                    name = kw.value.value
            names.add(name)
    return names


def instruction_files(root):
    # AGENTS.md and CONTRIBUTING.md must exist: a rename must not silently drop the check
    files = [root / 'AGENTS.md', root / 'CONTRIBUTING.md']
    optional = [root / '.github' / 'pull_request_template.md']
    optional.extend(sorted(root.glob('.claude/skills/*/SKILL.md')))
    return files + [f for f in optional if f.is_file()]


def check_instructions(text, root, sessions, base=None):
    """Return one message per unknown nox session or missing relative path."""
    base = base or root
    problems = []
    for line_number, line in enumerate(text.splitlines(), 1):
        for name in NOX_SESSION_RE.findall(line):
            if name not in sessions:
                problems.append(f'line {line_number}: missing nox session {name}')
        targets = [
            t for t in LINK_RE.findall(line) if not t.startswith(('http://', 'https://', '#'))
        ]
        targets += TICK_RE.findall(line)
        for target in targets:
            path = target.split('#')[0].rstrip('.,:;')
            if not path or PLACEHOLDER_RE.search(path):
                continue
            if not any((d / path).exists() for d in (root, base, root / 'b2sdk')):
                problems.append(f'line {line_number}: missing path {path}')
    return problems


@pytest.mark.parametrize('file', instruction_files(ROOT), ids=lambda f: str(f.relative_to(ROOT)))
def test_instructions_name_existing_sessions_and_paths(file):
    assert file.is_file(), f'{file.relative_to(ROOT)} is missing'
    text = file.read_text(encoding='utf-8')
    problems = check_instructions(text, ROOT, noxfile_sessions(ROOT / 'noxfile.py'), file.parent)
    assert not problems, f'{file.relative_to(ROOT)}: ' + '; '.join(problems)


def test_noxfile_sessions_are_read_statically():
    sessions = noxfile_sessions(ROOT / 'noxfile.py')
    assert {'format', 'lint', 'unit', 'towncrier_check'} <= sessions
    assert 'format_' not in sessions


def test_unknown_session_and_path_are_rejected(tmp_path):
    (tmp_path / 'real.md').write_text('x')
    text = (
        'Run `nox --non-interactive -s no_such_session-3.14`.\n'
        'See [a](real.md), [b](gone.md) and `test/nope.py`; `test/vN/*` is skipped.\n'
        'Also `gone.toml` and `real.md`.\n'
    )
    problems = check_instructions(text, tmp_path, {'lint'})
    assert problems == [
        'line 1: missing nox session no_such_session',
        'line 2: missing path gone.md',
        'line 2: missing path test/nope.py',
        'line 3: missing path gone.toml',
    ]
