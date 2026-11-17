######################################################################
#
# File: scripts/release_security.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Fail closed on the public GitHub security sources; print counts, never alert content."""

from __future__ import annotations

import json
import re
import subprocess
import sys

BLOCKING = {'medium', 'high', 'critical'}


def github_api(endpoint):
    result = subprocess.run(
        ['gh', 'api', '--paginate', '--slurp', endpoint],
        check=True,
        capture_output=True,
        text=True,
    )
    pages = json.loads(result.stdout)
    if not isinstance(pages, list) or not all(isinstance(page, list) for page in pages):
        raise ValueError('Invalid paginated response')
    return [item for page in pages for item in page]


def check(repo, api=github_api):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise ValueError('Expected owner/repository')
    holds = []
    for source in ('dependabot', 'code-scanning', 'secret-scanning'):
        try:
            items = api(f'repos/{repo}/{source}/alerts?state=open&per_page=100')
            if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
                raise ValueError('Invalid alert response')
            count = 0
            for item in items:
                if source == 'dependabot':
                    severity = item['security_advisory']['severity']
                    if severity not in BLOCKING | {'low'}:
                        raise ValueError('Unknown security severity')
                    count += severity in BLOCKING and bool(
                        item['security_vulnerability']['first_patched_version']
                    )
                elif source == 'code-scanning':
                    rule = item['rule']
                    severity = rule.get('security_severity_level')
                    if severity not in BLOCKING | {'low', None}:
                        raise ValueError('Unknown security severity')
                    if severity is None and 'security' in rule.get('tags', []):
                        raise ValueError('Security alert has no severity')
                    count += severity in BLOCKING
                else:
                    # The secret-scanning API has no severity. Every open credential exposure holds.
                    count += 1
            print(f'{source}: {len(items)} open; {count} blocking')
            if count:
                holds.append(f'{source}: {count} blocking alert(s)')
        except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, RuntimeError):
            holds.append(f'{source}: UNVERIFIED; cannot read or classify the required source')
    return holds


if __name__ == '__main__':
    for hold in (holds := check(sys.argv[1] if len(sys.argv) == 2 else 'Backblaze/b2-sdk-python')):
        print(f'HOLD: {hold}')
    sys.exit(bool(holds))
