######################################################################
#
# File: test/static/test_unit_tests_simulator.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""The unit-test jobs run the unit tests against the pinned b2-simulator."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).parents[2] / '.github' / 'workflows'
SIMULATOR_REPOSITORY = 'backblaze-labs/b2-simulator'
# b2-simulator v0.6.0.
SIMULATOR_PIN = '4052c6f8f45350486460d19bc796866cbdcdd3bc'
KEY_SET_FLAG = "${{ secrets.SIMULATOR_DEPLOY_KEY != '' }}"
SECRET_EXPRESSION = re.compile(r'secrets\.[A-Za-z_]')
# (workflow file, job id) of every job that runs `nox -vs unit`.
UNIT_JOBS = [('ci.yml', 'test'), ('cd.yml', 'test')]


def load(name):
    return yaml.safe_load((WORKFLOWS / name).read_text())


def step_named(job, name):
    [step] = [step for step in job['steps'] if step.get('name') == name]
    return step


def index_of(job, predicate):
    return next(i for i, step in enumerate(job['steps']) if predicate(step))


def simulator_checkouts(job):
    return [
        step
        for step in job['steps']
        if step.get('uses', '').startswith('actions/checkout@')
        and step.get('with', {}).get('repository') == SIMULATOR_REPOSITORY
    ]


def runs_unit_tests(step):
    # This repository's unit tests (the CLI job runs the CLI's with `uv run --directory`).
    return 'uv run nox -vs unit' in step.get('run', '')


def test_every_job_that_runs_unit_tests_is_listed():
    found = [
        (path.name, job_id)
        for path in sorted(WORKFLOWS.glob('*.yml'))
        for job_id, job in load(path.name)['jobs'].items()
        if any(runs_unit_tests(step) for step in job.get('steps', []))
    ]
    assert sorted(found) == sorted(UNIT_JOBS)


@pytest.mark.parametrize('workflow,job_id', UNIT_JOBS)
def test_the_pin_is_stated_once_per_workflow_and_checked_out(workflow, job_id):
    text = (WORKFLOWS / workflow).read_text()
    assert load(workflow)['env']['SIMULATOR_REVISION'] == SIMULATOR_PIN
    assert text.count(SIMULATOR_PIN) == 1
    job = load(workflow)['jobs'][job_id]
    [checkout] = simulator_checkouts(job)
    assert checkout['with']['ref'] == '${{ env.SIMULATOR_REVISION }}'
    assert checkout['with']['ssh-key'] == '${{ secrets.SIMULATOR_DEPLOY_KEY }}'
    assert checkout['with']['persist-credentials'] is False
    verify = step_named(job, 'Verify the b2-simulator revision')
    assert 'rev-parse HEAD)" = "$SIMULATOR_REVISION"' in verify['run']
    assert verify['shell'] == 'bash'


@pytest.mark.parametrize('workflow,job_id', UNIT_JOBS)
def test_the_unit_tests_get_the_checkout_and_node(workflow, job_id):
    job = load(workflow)['jobs'][job_id]
    [checkout] = simulator_checkouts(job)
    unit = index_of(job, runs_unit_tests)
    assert job['steps'][unit]['env']['B2_SIMULATOR_DIR'] == (
        '${{ github.workspace }}/' + checkout['with']['path']
    )
    node = index_of(job, lambda step: step.get('uses', '').startswith('actions/setup-node@'))
    assert job['steps'][node]['with']['node-version'] == 22
    assert index_of(job, lambda step: step is checkout) < unit
    assert node < unit
    # The SDK and the simulator are separate checkouts side by side.
    sdk = job['steps'][0 if workflow == 'cd.yml' else 1]
    assert sdk['uses'].startswith('actions/checkout@')
    assert sdk['with']['path'] != checkout['with']['path']
    assert job['steps'][unit]['working-directory'] == sdk['with']['path']


@pytest.mark.parametrize('workflow,job_id', UNIT_JOBS)
def test_a_missing_deploy_key_fails_and_says_which_secrets_to_add(workflow, job_id):
    job = load(workflow)['jobs'][job_id]
    assert job['env']['SIMULATOR_DEPLOY_KEY_SET'] == KEY_SET_FLAG
    guard = step_named(job, 'Require the b2-simulator deploy key')
    assert guard['if'] == "${{ env.SIMULATOR_DEPLOY_KEY_SET != 'true' }}"
    assert 'exit 1' in guard['run']
    assert 'Dependabot secret' in guard['run']
    assert 'SIMULATOR_DEPLOY_KEY' in guard['run']
    assert index_of(job, lambda step: step is guard) < index_of(
        job, lambda step: step in simulator_checkouts(job)
    )


@pytest.mark.parametrize('workflow,job_id', UNIT_JOBS)
def test_the_deploy_key_never_reaches_a_shell(workflow, job_id):
    job = load(workflow)['jobs'][job_id]
    for step in job['steps']:
        assert 'SIMULATOR_DEPLOY_KEY' not in str(step.get('env', {}))
        assert not SECRET_EXPRESSION.search(step.get('run', ''))
        if 'ssh-key' in step.get('with', {}):
            assert step in simulator_checkouts(job)


@pytest.mark.parametrize('workflow', ['ci.yml', 'cd.yml'])
def test_no_step_writes_github_env(workflow):
    assert 'GITHUB_ENV' not in (WORKFLOWS / workflow).read_text()


@pytest.mark.parametrize(
    'event,head,refused',
    [
        ('pull_request', 'someone/b2-sdk-python', True),
        ('pull_request', 'Backblaze/b2-sdk-python', False),
        ('push', 'Backblaze/b2-sdk-python', False),
    ],
)
def test_fork_pull_requests_are_refused_with_a_message(event, head, refused):
    job = load('ci.yml')['jobs']['test']
    guard = step_named(job, 'Require a pull request branch in this repository')
    assert job['steps'].index(guard) == 0
    assert guard['shell'] == 'bash'
    assert guard['env'] == {
        'EVENT_NAME': '${{ github.event_name }}',
        'HEAD_REPOSITORY': (
            '${{ github.event.pull_request.head.repo.full_name || github.repository }}'
        ),
    }
    completed = subprocess.run(
        ['bash', '-c', guard['run']],
        env={
            'PATH': os.environ['PATH'],
            'EVENT_NAME': event,
            'HEAD_REPOSITORY': head,
            'GITHUB_REPOSITORY': 'Backblaze/b2-sdk-python',
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert (completed.returncode != 0) == refused, completed
    if refused:
        assert 'b2-simulator' in completed.stdout
        assert 'branch in Backblaze/b2-sdk-python' in completed.stdout


def test_the_unit_matrix_is_unchanged():
    matrix = load('ci.yml')['jobs']['test']['strategy']['matrix']
    assert matrix['os'] == ['ubuntu-latest', 'macos-latest', 'windows-latest']
    assert matrix['python-version'] == ['3.10', '3.11', '3.12', '3.13', '3.14', 'pypy3.10']
