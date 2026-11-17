######################################################################
#
# File: scripts/release_preflight.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Require successful master CI and, for a real release, a tag on master history."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

from prepare_release import VERSION_PATTERN


def require_green_ci(repo, sha):
    result = subprocess.run(
        ['gh', 'api', f'repos/{repo}/actions/workflows/ci.yml/runs?head_sha={sha}&per_page=100'],
        check=True,
        capture_output=True,
        text=True,
    )
    runs = [
        run
        for run in json.loads(result.stdout)['workflow_runs']
        if run['head_sha'] == sha and run['head_branch'] == 'master' and run['event'] == 'push'
    ]
    if not runs or runs[0]['status'] != 'completed' or runs[0]['conclusion'] != 'success':
        raise RuntimeError(
            'The latest master CI run for this commit must have completed successfully'
        )


def main():
    repo = os.environ['GITHUB_REPOSITORY']
    if repo != 'Backblaze/b2-sdk-python':
        raise RuntimeError('Release workflow is restricted to Backblaze/b2-sdk-python')
    subprocess.run(['git', 'fetch', '--no-tags', 'origin', 'master'], check=True)

    def git(*args):
        return subprocess.run(
            ['git', *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    master = git('rev-parse', 'refs/remotes/origin/master')
    require_green_ci(repo, master)
    if os.environ['GITHUB_EVENT_NAME'] == 'workflow_dispatch':
        if os.environ.get('INPUT_DRY_RUN') != 'true':
            raise RuntimeError('Manual dispatch supports dry_run=true only; it never publishes')
        print('Non-publishing dry run; master CI is green.')
        return
    if os.environ.get('GITHUB_REF_TYPE') != 'tag':
        raise RuntimeError('A release requires a tag push')
    tag = os.environ['GITHUB_REF_NAME']
    if not re.fullmatch('v' + VERSION_PATTERN, tag):
        raise RuntimeError('Invalid release tag')
    head = git('rev-parse', 'HEAD')
    if git('rev-parse', f'refs/tags/{tag}^{{commit}}') != head:
        raise RuntimeError('Checked-out revision differs from the release tag')
    subprocess.run(['git', 'merge-base', '--is-ancestor', head, master], check=True)
    require_green_ci(repo, head)
    print('Release tag is on master history and both release and master CI are green.')


if __name__ == '__main__':
    try:
        main()
    except (KeyError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        # Do not echo API payloads or credential-bearing stderr.
        sys.exit(f'Release preflight failed ({type(error).__name__})')
