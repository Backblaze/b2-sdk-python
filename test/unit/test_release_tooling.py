######################################################################
#
# File: test/unit/test_release_tooling.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import json
import runpy
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

ROOT = Path(__file__).parents[2]


def tooling(name):
    with patch.object(sys, 'path', [str(ROOT / 'scripts'), *sys.path]):
        return runpy.run_path(str(ROOT / 'scripts' / f'{name}.py'))


@pytest.fixture
def release_repo(tmp_path):
    def git(*args):
        return subprocess.run(
            ['git', *args], cwd=tmp_path, check=True, capture_output=True, text=True
        )

    git('init', '--initial-branch=master')
    git('config', 'user.name', 'Release test')
    git('config', 'user.email', 'release-test@example.invalid')
    git('config', 'commit.gpgsign', 'false')
    git('config', 'tag.gpgsign', 'false')
    git('commit', '--allow-empty', '-m', 'Initial fixture')
    return tmp_path, git


def test_release_preparation_refuses_other_branches(release_repo):
    root, git = release_repo
    git('switch', '-c', 'feature')
    with pytest.raises(RuntimeError, match='master'):
        tooling('prepare_release')['prepare']('2.13.5', root)


def test_release_preparation_refuses_untracked_changes(release_repo):
    root, _ = release_repo
    (root / 'pending').write_text('pending')
    with pytest.raises(RuntimeError, match='clean'):
        tooling('prepare_release')['prepare']('2.13.5', root)


@pytest.mark.parametrize('version', ['v2.13.5', '2.13', '2.13.5;echo unsafe', '2.13.5\n'])
def test_release_preparation_rejects_invalid_versions(release_repo, version):
    root, _ = release_repo
    with pytest.raises(ValueError, match='X.Y.Z'):
        tooling('prepare_release')['prepare'](version, root)


def test_release_preparation_refuses_an_existing_tag(release_repo):
    root, git = release_repo
    git('tag', 'v2.13.5')
    with pytest.raises(RuntimeError, match='exists'):
        tooling('prepare_release')['prepare']('2.13.5', root)


def test_release_preparation_batches_then_merges_without_tagging(release_repo):
    root, git = release_repo
    calls = []

    def run(args, **kwargs):
        if args[0] == 'go':
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, '')
        return subprocess.run(args, **kwargs)

    tooling('prepare_release')['prepare']('2.13.5', root, run=run)
    assert [call[3:] for call in calls] == [['batch', 'v2.13.5'], ['merge']]
    assert git('tag', '--list').stdout == ''


@pytest.mark.parametrize('version', ['2.14.0-a.1', '2.14.0-b.1', '2.14.0-rc.1', '2.14.0-rc1'])
def test_release_preparation_preserves_the_prerelease_version(release_repo, version):
    root, _ = release_repo
    calls = []

    def run(args, **kwargs):
        if args[0] == 'go':
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, '')
        return subprocess.run(args, **kwargs)

    tooling('prepare_release')['prepare'](version, root, run=run)
    assert calls[0][3:] == ['batch', f'v{version}']


@pytest.mark.parametrize(
    'version', ['2.14.0-preview.1', '2.14.0-rc-1', '2.14.0-rc..1', '2.1\u0664.0']
)
def test_release_preparation_rejects_unsupported_prerelease_schemes(release_repo, version):
    root, _ = release_repo

    def run(args, **kwargs):
        if args[0] == 'go':
            return subprocess.CompletedProcess(args, 0, '')
        return subprocess.run(args, **kwargs)

    with pytest.raises(ValueError, match='X.Y.Z'):
        tooling('prepare_release')['prepare'](version, root, run=run)


def test_version_error_explains_all_supported_stages(release_repo):
    root, _ = release_repo
    with pytest.raises(ValueError) as error:
        tooling('prepare_release')['prepare']('unsupported', root)
    assert all(stage in str(error.value) for stage in ('a.N', 'b.N', 'rc.N'))


def test_github_no_gpg_keys_response_does_not_prevent_ssh_verification(release_repo, monkeypatch):
    root, git = release_repo
    key = root / 'fixture-key'
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    git(
        '-c',
        'gpg.format=ssh',
        '-c',
        f'user.signingkey={key}',
        'tag',
        '-s',
        'v2.13.5',
        '-m',
        'Fixture',
    )
    public_key = key.with_suffix('.pub').read_text()
    real_run, real_which = subprocess.run, shutil.which

    def run(args, **kwargs):
        if args[0] == 'gpg':
            if kwargs.get('check'):
                raise subprocess.CalledProcessError(2, args)
            return subprocess.CompletedProcess(args, 2)
        return real_run(args, **kwargs)

    def fetch(url):
        if url.endswith('.keys'):
            return public_key
        return (
            "-----BEGIN PGP PUBLIC KEY BLOCK-----\nNote: This user hasn't uploaded any GPG keys.\n"
        )

    monkeypatch.setattr(subprocess, 'run', run)
    monkeypatch.setattr(shutil, 'which', lambda name: 'gpg' if name == 'gpg' else real_which(name))
    assert tooling('verify_release_tag')['verify'](
        'v2.13.5', root, enforce=True, maintainers=['example'], fetch=fetch
    )


def alerts(dependabot=None, code=None, secrets=None):
    def api(endpoint):
        if '/dependabot/' in endpoint:
            return dependabot or []
        if '/code-scanning/' in endpoint:
            return code or []
        if '/secret-scanning/' in endpoint:
            return secrets or []
        raise AssertionError('Unexpected endpoint')

    return api


def test_security_gate_accepts_empty_sources():
    assert tooling('release_security')['check']('owner/repo', api=alerts()) == []


@pytest.mark.parametrize('severity', ['medium', 'high', 'critical'])
def test_security_gate_blocks_fixable_dependabot_alerts(severity):
    source = {
        'security_advisory': {'severity': severity},
        'security_vulnerability': {'first_patched_version': {'identifier': '9.0'}},
    }
    assert tooling('release_security')['check']('owner/repo', api=alerts(dependabot=[source]))


def test_security_gate_does_not_block_low_or_unpatched_dependabot_alerts():
    sources = [
        {
            'security_advisory': {'severity': 'low'},
            'security_vulnerability': {'first_patched_version': {'identifier': '9.0'}},
        },
        {
            'security_advisory': {'severity': 'high'},
            'security_vulnerability': {'first_patched_version': None},
        },
    ]
    assert tooling('release_security')['check']('owner/repo', api=alerts(dependabot=sources)) == []


@pytest.mark.parametrize('severity', ['medium', 'high', 'critical'])
def test_security_gate_blocks_code_scanning_alerts(severity):
    source = {'tool': {'name': 'CodeQL'}, 'rule': {'security_severity_level': severity}}
    assert tooling('release_security')['check']('owner/repo', api=alerts(code=[source]))


def test_security_gate_applies_the_codeql_policy_to_codeql_only():
    source = {'tool': {'name': 'Other scanner'}, 'rule': {'security_severity_level': 'high'}}
    assert tooling('release_security')['check']('owner/repo', api=alerts(code=[source])) == []


def test_security_gate_requests_secret_metadata_without_secret_values():
    endpoints = []

    def api(endpoint):
        endpoints.append(endpoint)
        return []

    assert tooling('release_security')['check']('owner/repo', api=api) == []
    assert any('/secret-scanning/' in url and 'hide_secret=true' in url for url in endpoints)


def test_release_security_credentials_have_a_protected_environment_boundary():
    workflow = yaml.load((ROOT / '.github/workflows/cd.yml').read_text(), Loader=yaml.BaseLoader)
    gates = workflow['jobs']['release-gates']
    assert gates.get('environment') == 'release-security'
    assert gates['permissions']['contents'] == 'read'
    assert 'id-token' not in gates['permissions']
    for name in ('publish-pypi', 'github-release'):
        assert "github.event_name == 'push'" in workflow['jobs'][name]['if']


def test_supported_prerelease_tags_retain_the_pypi_publish_path():
    workflow = yaml.load((ROOT / '.github/workflows/cd.yml').read_text(), Loader=yaml.BaseLoader)
    condition = workflow['jobs']['publish-pypi']['if']
    assert condition == "github.event_name == 'push'"


def test_release_preflight_retains_safe_actionable_failure_reasons():
    module = tooling('release_preflight')
    hold = module['ReleaseHold']('Current master CI is not successful')
    assert 'Current master CI is not successful' in module['failure_reason'](hold)


def test_security_gate_blocks_open_secret_scanning_alerts():
    assert tooling('release_security')['check']('owner/repo', api=alerts(secrets=[{}]))


def test_security_gate_fails_closed_when_a_source_is_unreadable():
    def denied(endpoint):
        raise RuntimeError('Access denied')

    assert len(tooling('release_security')['check']('owner/repo', api=denied)) == 3


def test_security_gate_fails_closed_on_malformed_response():
    assert tooling('release_security')['check']('owner/repo', api=lambda endpoint: {})


@pytest.mark.parametrize(
    'value, expected', [('', False), ('false', False), ('true', True), ('on', True)]
)
def test_signed_tag_enforcement_rollout(value, expected):
    assert tooling('verify_release_tag')['enforcing'](value) is expected


def test_signed_tag_enforcement_rejects_a_misspelled_setting():
    with pytest.raises(ValueError):
        tooling('verify_release_tag')['enforcing']('tru')


def test_unsigned_tag_is_warning_only_until_enrolled(release_repo):
    root, git = release_repo
    git('tag', '-a', 'v2.13.5', '-m', 'Unsigned fixture')
    verify = tooling('verify_release_tag')['verify']
    assert verify('v2.13.5', root, enforce=False, maintainers=['example'], fetch=lambda url: '')
    assert not verify('v2.13.5', root, enforce=True, maintainers=['example'], fetch=lambda url: '')


def test_ssh_signed_tag_requires_an_enrolled_key(release_repo, tmp_path):
    root, git = release_repo
    # Generated fixture material stays in pytest's temporary directory.
    key = tmp_path / 'fixture-signing-key'
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    git(
        '-c',
        'gpg.format=ssh',
        '-c',
        f'user.signingkey={key}',
        'tag',
        '-s',
        'v2.13.5',
        '-m',
        'Fixture',
    )
    verify = tooling('verify_release_tag')['verify']
    public_key = key.with_suffix('.pub').read_text()

    # Key files are untracked fixtures; signature verification itself does not mutate the repo.
    def fetch(url):
        return public_key if url.endswith('.keys') else ''

    assert verify('v2.13.5', root, enforce=True, maintainers=['example'], fetch=fetch)
    assert not verify('v2.13.5', root, enforce=True, maintainers=['example'], fetch=lambda url: '')

    # A pre-existing local/global signer configuration cannot expand enrollment.
    external = root / 'external-signers'
    external.write_text(f'external {public_key}')
    git('config', 'gpg.ssh.allowedSignersFile', str(external))
    assert not verify('v2.13.5', root, enforce=True, maintainers=['example'], fetch=lambda url: '')


@pytest.mark.parametrize(
    'value', [None, {}, {'kind': 'Fixed'}, {'kind': 'Unknown', 'body': 'Entry.'}]
)
def test_changie_fragment_validation_rejects_incomplete_entries(value):
    with pytest.raises(ValueError):
        tooling('check_changelog')['validate_fragment'](value)


def test_changie_fragment_validation_accepts_a_complete_entry():
    tooling('check_changelog')['validate_fragment'](
        {'kind': 'Fixed', 'body': 'Correct a release check.', 'time': '2026-11-17T00:00:00Z'}
    )


@pytest.mark.parametrize(
    'status, conclusion, branch, event',
    [
        ('completed', 'failure', 'master', 'push'),
        ('in_progress', None, 'master', 'push'),
        ('completed', 'success', 'feature', 'push'),
        ('completed', 'success', 'master', 'pull_request'),
    ],
)
def test_release_preflight_rejects_red_pending_or_unrelated_ci(
    monkeypatch, status, conclusion, branch, event
):
    function = tooling('release_preflight')['require_green_ci']
    response = {
        'workflow_runs': [
            {
                'head_sha': 'abc',
                'head_branch': branch,
                'event': event,
                'status': status,
                'conclusion': conclusion,
            }
        ]
    }
    monkeypatch.setattr(
        subprocess,
        'run',
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(response)),
    )
    with pytest.raises(RuntimeError, match='successfully'):
        function('owner/repo', 'abc')


def test_release_preflight_requires_the_latest_master_run_to_pass(monkeypatch):
    function = tooling('release_preflight')['require_green_ci']
    response = {
        'workflow_runs': [
            {
                'head_sha': 'abc',
                'head_branch': 'master',
                'event': 'push',
                'status': 'completed',
                'conclusion': 'failure',
            },
            {
                'head_sha': 'abc',
                'head_branch': 'master',
                'event': 'push',
                'status': 'completed',
                'conclusion': 'success',
            },
        ]
    }
    monkeypatch.setattr(
        subprocess,
        'run',
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(response)),
    )
    with pytest.raises(RuntimeError):
        function('owner/repo', 'abc')


def test_release_preflight_accepts_successful_master_ci(monkeypatch):
    function = tooling('release_preflight')['require_green_ci']
    response = {
        'workflow_runs': [
            {
                'head_sha': 'abc',
                'head_branch': 'master',
                'event': 'push',
                'status': 'completed',
                'conclusion': 'success',
            }
        ]
    }
    monkeypatch.setattr(
        subprocess,
        'run',
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(response)),
    )
    function('owner/repo', 'abc')


@pytest.mark.parametrize('merged', [False, True])
def test_release_preflight_requires_tagged_commit_on_master(release_repo, monkeypatch, merged):
    root, git = release_repo
    base = git('rev-parse', 'HEAD').stdout.strip()
    git('update-ref', 'refs/remotes/origin/master', base)
    if not merged:
        git('switch', '-c', 'unmerged-release')
        git('commit', '--allow-empty', '-m', 'Unmerged fixture')
    git('tag', '-a', 'v2.13.5', '-m', 'Fixture')
    function = tooling('release_preflight')['main']
    function.__globals__['require_green_ci'] = lambda repo, sha: None
    real_run = subprocess.run

    def run(args, **kwargs):
        if args[:2] == ['git', 'fetch']:
            return subprocess.CompletedProcess(args, 0)
        return real_run(args, **kwargs)

    monkeypatch.setattr(subprocess, 'run', run)
    monkeypatch.chdir(root)
    monkeypatch.setenv('GITHUB_REPOSITORY', 'Backblaze/b2-sdk-python')
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'push')
    monkeypatch.setenv('GITHUB_REF_TYPE', 'tag')
    monkeypatch.setenv('GITHUB_REF_NAME', 'v2.13.5')
    if merged:
        function()
    else:
        with pytest.raises(subprocess.CalledProcessError):
            function()


def test_dispatch_cannot_enable_publishing(release_repo, monkeypatch):
    root, git = release_repo
    git('update-ref', 'refs/remotes/origin/master', git('rev-parse', 'HEAD').stdout.strip())
    function = tooling('release_preflight')['main']
    function.__globals__['require_green_ci'] = lambda repo, sha: None
    real_run = subprocess.run

    def run(args, **kwargs):
        if args[:2] == ['git', 'fetch']:
            return subprocess.CompletedProcess(args, 0)
        return real_run(args, **kwargs)

    monkeypatch.setattr(subprocess, 'run', run)
    monkeypatch.chdir(root)
    monkeypatch.setenv('GITHUB_REPOSITORY', 'Backblaze/b2-sdk-python')
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'workflow_dispatch')
    monkeypatch.setenv('INPUT_DRY_RUN', 'false')
    with pytest.raises(RuntimeError, match='never publishes'):
        function()


def test_release_notes_extracts_exact_version_without_running_content(tmp_path):
    changelog = tmp_path / 'CHANGELOG.md'
    changelog.write_text('## [2.13.5] - today\n\n### Fixed\n\n- Entry.\n\n## [2.13.4]\n\n- Old.\n')
    result = subprocess.run(
        [
            'awk',
            '-v',
            'version=2.13.5',
            '-f',
            str(ROOT / 'scripts/release-notes.awk'),
            str(changelog),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert 'Entry.' in result.stdout
    assert 'Old.' not in result.stdout


def test_release_notes_fails_when_version_is_missing(tmp_path):
    changelog = tmp_path / 'CHANGELOG.md'
    changelog.write_text('## [2.13.4]\n')
    result = subprocess.run(
        [
            'awk',
            '-v',
            'version=2.13.5',
            '-f',
            str(ROOT / 'scripts/release-notes.awk'),
            str(changelog),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
