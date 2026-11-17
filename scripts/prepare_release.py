######################################################################
#
# File: scripts/prepare_release.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Prepare release notes on a clean master; never commit, tag, or publish."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

CHANGIE = ['go', 'run', 'github.com/miniscruff/changie@v1.24.0']


def prepare(version, root, run=subprocess.run):
    if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?', version):
        raise ValueError('Version must be X.Y.Z or X.Y.Z-rc.1 without a leading v')

    def git(*args):
        return run(
            ['git', *args], cwd=root, check=True, capture_output=True, text=True
        ).stdout.strip()

    if git('branch', '--show-current') != 'master':
        raise RuntimeError('Release preparation requires master')
    if git('status', '--porcelain'):
        raise RuntimeError(
            'Release preparation requires a clean worktree, including untracked files'
        )
    if git('tag', '--list', f'v{version}'):
        raise RuntimeError('That release tag already exists')
    run([*CHANGIE, 'batch', f'v{version}'], cwd=root, check=True)
    run([*CHANGIE, 'merge'], cwd=root, check=True)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit('Usage: python scripts/prepare_release.py X.Y.Z')
    try:
        prepare(sys.argv[1], Path.cwd())
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
    print(
        'Release notes prepared. Create a release branch and PR; follow RELEASE.md after approval.'
    )
