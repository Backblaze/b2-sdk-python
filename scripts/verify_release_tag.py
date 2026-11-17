######################################################################
#
# File: scripts/verify_release_tag.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Verify tags using the enrolled maintainers' GitHub public GPG and SSH keys."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from release_versions import VERSION_PATTERN


def enforcing(value):
    normalized = value.strip().lower()
    if normalized not in {'', 'false', 'off', '0', 'true', 'on', '1'}:
        raise ValueError('ENFORCE_SIGNED_TAGS must be true or false')
    return normalized in {'true', 'on', '1'}


def fetch_keys(url):
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            content = response.read(1024 * 1024 + 1)
        if len(content) > 1024 * 1024:
            raise ValueError('Public key response exceeds size limit')
        return content.decode('utf-8')
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return ''
        raise


def verify(tag, root, *, enforce, maintainers, fetch=fetch_keys):
    verified = False
    try:
        if not re.fullmatch('v' + VERSION_PATTERN, tag):
            raise ValueError('Invalid release tag')
        if not maintainers or any(not re.fullmatch(r'[A-Za-z0-9-]+', name) for name in maintainers):
            raise ValueError('Invalid maintainer list')
        with tempfile.TemporaryDirectory(prefix='b2sdk-signers-') as temporary:
            home = Path(temporary)
            home.chmod(0o700)
            ssh_keys = []
            for name in maintainers:
                gpg_keys = fetch(f'https://github.com/{name}.gpg')
                if gpg_keys and shutil.which('gpg'):
                    # GitHub returns an armored stub for accounts with no GPG keys.
                    # An unsuccessful import must still allow an enrolled SSH key.
                    subprocess.run(
                        ['gpg', '--batch', '--homedir', str(home), '--import'],
                        input=gpg_keys,
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                for key in fetch(f'https://github.com/{name}.keys').splitlines():
                    if key.strip():
                        ssh_keys.append(f'{name} {key}')
            signers = home / 'allowed_signers'
            signers.write_text('\n'.join(ssh_keys) + '\n')
            commands = [
                [
                    'git',
                    '-c',
                    'gpg.format=openpgp',
                    '-c',
                    f'gpg.ssh.allowedSignersFile={signers}',
                    'verify-tag',
                    tag,
                ],
                [
                    'git',
                    '-c',
                    'gpg.format=ssh',
                    '-c',
                    f'gpg.ssh.allowedSignersFile={signers}',
                    'verify-tag',
                    tag,
                ],
            ]
            for command in commands:
                result = subprocess.run(
                    command,
                    cwd=root,
                    env={**os.environ, 'GNUPGHOME': str(home)},
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    verified = True
                    break
    except (OSError, ValueError, subprocess.SubprocessError, urllib.error.URLError):
        verified = False
    if verified:
        print('Tag signature verified against an enrolled GitHub maintainer key.')
    else:
        level = 'error' if enforce else 'warning'
        print(
            f'::{level}::Tag signature unverified; enroll maintainer keys before enabling enforcement.'
        )
    return verified or not enforce


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[1]
    names = [
        line.strip()
        for line in (root / '.github/signing-maintainers').read_text().splitlines()
        if line.strip() and not line.startswith('#')
    ]
    try:
        result = verify(
            sys.argv[1],
            root,
            enforce=enforcing(os.getenv('ENFORCE_SIGNED_TAGS', '')),
            maintainers=names,
        )
    except (IndexError, ValueError):
        sys.exit('Invalid tag or ENFORCE_SIGNED_TAGS setting')
    sys.exit(not result)
