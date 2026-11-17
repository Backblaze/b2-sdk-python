######################################################################
#
# File: scripts/check_changelog.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Validate fragments and require a reproducible Changie merge without modifying files."""

from __future__ import annotations

import argparse
import datetime
import subprocess
from pathlib import Path

CHANGIE = ['go', 'run', 'github.com/miniscruff/changie@v1.24.0']
KINDS = {'Added', 'Changed', 'Deprecated', 'Removed', 'Fixed', 'Doc', 'Infrastructure', 'Security'}


def validate_fragment(value):
    if not isinstance(value, dict) or value.get('kind') not in KINDS:
        raise ValueError('A fragment needs a configured kind')
    body = value.get('body')
    if not isinstance(body, str) or not body.strip():
        raise ValueError('A fragment needs a nonempty body')
    time = value.get('time')
    if not isinstance(time, (datetime.datetime, str)):
        raise ValueError('A fragment needs a timestamp')
    datetime.datetime.fromisoformat(str(time).replace('Z', '+00:00'))


def main():
    import yaml

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base')
    parser.add_argument('--allow-missing-fragment', action='store_true')
    args = parser.parse_args()
    root = Path.cwd()
    for path in (root / '.changes/unreleased').iterdir():
        if path.name == '.gitkeep':
            continue
        if path.is_symlink() or not path.is_file() or path.suffix not in {'.yaml', '.yml'}:
            raise ValueError('Unreleased entries must be regular YAML files')
        if path.stat().st_size > 16 * 1024:
            raise ValueError('Fragment exceeds 16 KiB')
        validate_fragment(yaml.safe_load(path.read_text()))
    merged = subprocess.run(
        [*CHANGIE, 'merge', '--dry-run'], check=True, capture_output=True, text=True
    )
    if merged.stdout != (root / 'CHANGELOG.md').read_text():
        raise ValueError('CHANGELOG.md differs from changie merge; regenerate it')
    subprocess.run([*CHANGIE, 'batch', 'patch', '--dry-run'], check=True, capture_output=True)
    if args.base and not args.allow_missing_fragment:
        diff = subprocess.run(
            ['git', 'diff', '--name-status', f'{args.base}...HEAD'],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        added = [line.split('\t')[-1] for line in diff if line.startswith('A\t')]
        paths = {line.split('\t')[-1] for line in diff}
        fragment = any(
            path.startswith('.changes/unreleased/') and path.endswith(('.yaml', '.yml'))
            for path in added
        )
        release = 'CHANGELOG.md' in paths and any(
            Path(path).parent == Path('.changes')
            and path.startswith('.changes/v')
            and path.endswith('.md')
            for path in added
        )
        if not fragment and not release and paths != {'uv.lock'}:
            raise ValueError('Add a Changie fragment, or batch a release with its changelog')
    print('Changelog checks passed')


if __name__ == '__main__':
    main()
