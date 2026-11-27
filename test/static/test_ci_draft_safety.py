######################################################################
#
# File: test/static/test_ci_draft_safety.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Keep PR integration on Simulator and live B2 on a weekly/manual/release path."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
WORKFLOWS = ROOT / '.github/workflows'
CI_TEXT = (WORKFLOWS / 'ci.yml').read_text()
SIMULATOR_PATH = WORKFLOWS / 'simulator-integration.yml'
LIVE_PATH = WORKFLOWS / 'live-b2.yml'
LIVE_TESTS_PATH = WORKFLOWS / 'live-b2-tests.yml'
KEYS = ('B2_TEST_APPLICATION_KEY', 'B2_TEST_APPLICATION_KEY_ID')
SIMULATOR_REPOSITORY = 'backblaze-labs/b2-simulator'
SIMULATOR_PIN = 'a1e1db373b71f2d5f40082c19bc716cb6b3c3675'
CLI_REPOSITORY = 'Backblaze/B2_Command_Line_Tool'
KEY_SET_FLAG = "${{ secrets.SIMULATOR_DEPLOY_KEY != '' }}"
SECRET_EXPRESSION = re.compile(r'secrets\.[A-Za-z_]')


def triggers(workflow):
    return workflow.get('on', workflow.get(True))


def load(path):
    return yaml.safe_load(path.read_text())


def steps(workflow):
    return [step for job in workflow['jobs'].values() for step in job.get('steps', [])]


def called_workflows(workflow):
    """Load the local reusable workflows that a workflow's jobs call."""
    return [
        load(ROOT / job['uses'])
        for job in workflow['jobs'].values()
        if job.get('uses', '').startswith('./')
    ]


def scripts(workflow, follow_calls=False):
    workflows = [workflow, *(called_workflows(workflow) if follow_calls else [])]
    return '\n'.join(step.get('run', '') for wf in workflows for step in steps(wf))


def checkouts_of(job, repository):
    return [
        step
        for step in job.get('steps', [])
        if step.get('uses', '').startswith('actions/checkout@')
        and step.get('with', {}).get('repository') == repository
    ]


def test_regular_ci_cannot_use_real_b2_credentials():
    assert 'B2_LIVE_TESTS_ENABLED' not in CI_TEXT
    for key in KEYS:
        assert not re.search(rf'secrets\.{key}\b', CI_TEXT)


def test_simulator_workflow_runs_on_every_pull_request_and_fails_closed():
    text = SIMULATOR_PATH.read_text()
    workflow = yaml.safe_load(text)
    events = triggers(workflow)
    assert 'pull_request' in events
    assert 'schedule' not in events
    assert 'SIMULATOR_DEPLOY_KEY' in text
    run = scripts(workflow)
    assert 'B2_TEST_ENVIRONMENT' in run
    assert 'uv run nox -vs integration' in run
    assert 'exit 1' in run


def test_simulator_pin_is_stated_once_on_simulator_main_and_referenced():
    text = SIMULATOR_PATH.read_text()
    workflow = yaml.safe_load(text)
    assert workflow['env']['SIMULATOR_REVISION'] == SIMULATOR_PIN
    assert text.count(SIMULATOR_PIN) == 1
    checkouts = [
        step
        for job in workflow['jobs'].values()
        for step in checkouts_of(job, SIMULATOR_REPOSITORY)
    ]
    assert checkouts
    for step in checkouts:
        assert step['with']['ref'] == '${{ env.SIMULATOR_REVISION }}'
        assert step['with']['persist-credentials'] is False


def test_simulator_key_presence_is_checked_with_if_not_in_a_shell():
    workflow = load(SIMULATOR_PATH)
    jobs = [job for job in workflow['jobs'].values() if checkouts_of(job, SIMULATOR_REPOSITORY)]
    assert jobs
    for job in jobs:
        assert job['env']['SIMULATOR_DEPLOY_KEY_SET'] == KEY_SET_FLAG
        guard = job['steps'][0]
        assert guard['if'] == "${{ env.SIMULATOR_DEPLOY_KEY_SET != 'true' }}"
        assert 'exit 1' in guard['run']
        for step in job['steps']:
            assert 'SIMULATOR_DEPLOY_KEY' not in str(step.get('env', {}))
            assert not SECRET_EXPRESSION.search(step.get('run', ''))
            ssh_key = step.get('with', {}).get('ssh-key')
            if ssh_key is not None:
                assert step in checkouts_of(job, SIMULATOR_REPOSITORY)


def test_dependabot_runs_the_simulator_and_is_told_which_secret_is_missing():
    text = SIMULATOR_PATH.read_text()
    workflow = yaml.safe_load(text)
    for job in workflow['jobs'].values():
        assert 'dependabot' not in str(job.get('if', '')).lower()
        for step in job.get('steps', []):
            assert 'dependabot' not in str(step.get('if', '')).lower()
    guards = [
        step['run']
        for job in workflow['jobs'].values()
        for step in job.get('steps', [])
        if 'SIMULATOR_DEPLOY_KEY_SET' in str(step.get('if', ''))
    ]
    assert guards
    for run in guards:
        assert 'admin' in run
        assert 'Dependabot secret' in run
        assert 'SIMULATOR_DEPLOY_KEY' in run


def test_cli_compatibility_secret_tests_run_against_the_simulator_in_pr_ci():
    workflow = load(SIMULATOR_PATH)
    cli_jobs = [
        job
        for job in workflow['jobs'].values()
        if checkouts_of(job, CLI_REPOSITORY) and checkouts_of(job, SIMULATOR_REPOSITORY)
    ]
    assert len(cli_jobs) == 1
    [job] = cli_jobs
    assert job['needs'] == 'simulator-access-policy'
    assert job['env']['INSTALL_SDK_FROM']
    assert checkouts_of(job, CLI_REPOSITORY)[0]['with']['persist-credentials'] is False
    run = '\n'.join(step.get('run', '') for step in job['steps'])
    assert 'B2_TEST_ENVIRONMENT' in run
    assert re.search(r'nox -vs integration .*-m "?require_secrets"?', run)


def test_live_b2_workflow_is_weekly_manual_and_fails_closed():
    text = LIVE_PATH.read_text()
    workflow = yaml.safe_load(text)
    assert set(triggers(workflow)) == {'schedule', 'workflow_dispatch'}
    assert triggers(workflow)['schedule']
    assert all(f'secrets.{key}' in text for key in KEYS)
    run = scripts(workflow, follow_calls=True)
    assert 'uv run nox -vs integration' in run
    assert 'exit 1' in run


def test_live_b2_tests_are_reusable_serialized_and_fail_closed():
    workflow = load(LIVE_TESTS_PATH)
    events = triggers(workflow)
    assert set(events) == {'workflow_call'}
    assert set(events['workflow_call']['secrets']) == set(KEYS)
    assert workflow['permissions'] == {'contents': 'read'}
    [job] = workflow['jobs'].values()
    # One group per caller and ref: a newer queued run must never replace a pending release run.
    assert job['concurrency'] == {
        'group': 'live-b2-${{ github.workflow }}-${{ github.ref }}',
        'cancel-in-progress': False,
    }
    assert job['env']['LIVE_B2_CREDENTIALS_SET'] == (
        "${{ secrets.B2_TEST_APPLICATION_KEY_ID != '' && secrets.B2_TEST_APPLICATION_KEY != '' }}"
    )
    guard = job['steps'][0]
    assert guard['if'] == "${{ env.LIVE_B2_CREDENTIALS_SET != 'true' }}"
    assert 'exit 1' in guard['run']
    assert not SECRET_EXPRESSION.search(guard['run'])


def test_live_b2_tests_cover_the_sdk_suite_and_cli_compatibility():
    workflow = load(LIVE_TESTS_PATH)
    cli_input = triggers(workflow)['workflow_call']['inputs']['cli-compat']
    assert (cli_input['type'], cli_input['required'], cli_input['default']) == (
        'boolean',
        False,
        False,
    )
    [job] = workflow['jobs'].values()
    assert job['env']['INSTALL_SDK_FROM']
    sdk_run = '\n'.join(step.get('run', '') for step in job['steps'] if 'if' not in step)
    assert 'nox -vs cleanup_old_buckets' in sdk_run
    assert 'uv run nox -vs integration' in sdk_run
    cli_steps = [step for step in job['steps'] if 'inputs.cli-compat' in str(step.get('if', ''))]
    assert any(step in checkouts_of(job, CLI_REPOSITORY) for step in cli_steps)
    cli_run = '\n'.join(step.get('run', '') for step in cli_steps)
    assert re.search(r'nox -vs integration .*-m "?require_secrets"? .*--cleanup', cli_run, re.S)


def test_weekly_live_b2_runs_cli_compatibility_and_release_does_not():
    weekly = load(LIVE_PATH)['jobs']['live-b2']
    assert weekly['with'] == {'cli-compat': True}
    release = load(WORKFLOWS / 'cd.yml')['jobs']['live-b2']
    assert 'cli-compat' not in release.get('with', {})


def test_weekly_live_b2_reports_scheduled_failures_with_a_scoped_token():
    workflow = load(LIVE_PATH)
    assert workflow['permissions'] == {'contents': 'read'}
    jobs = workflow['jobs']
    live = [
        name
        for name, job in jobs.items()
        if job.get('uses') == './' + str(LIVE_TESTS_PATH.relative_to(ROOT))
    ]
    assert len(live) == 1
    assert jobs[live[0]]['secrets'] == {key: f'${{{{ secrets.{key} }}}}' for key in KEYS}
    reporters = [job for job in jobs.values() if job.get('permissions', {}).get('issues')]
    assert len(reporters) == 1
    [reporter] = reporters
    assert reporter['permissions'] == {'issues': 'write'}
    assert reporter['needs'] == live[0]
    # Report any scheduled run that did not succeed, including cancelled or timed-out runs.
    assert reporter['if'] == (
        "${{ always() && github.event_name == 'schedule' && needs.%s.result != 'success' }}"
        % live[0]
    )
    run = '\n'.join(step.get('run', '') for step in reporter['steps'])
    assert 'gh issue create' in run
    assert 'gh issue comment' in run
