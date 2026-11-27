######################################################################
#
# File: test/static/test_ci_draft_safety.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Keep PR integration on Simulator and live B2 on a weekly/manual path."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
CI_TEXT = (ROOT / '.github/workflows/ci.yml').read_text()
SIMULATOR_PATH = ROOT / '.github/workflows/simulator-integration.yml'
LIVE_PATH = ROOT / '.github/workflows/live-b2.yml'
KEYS = ('B2_TEST_APPLICATION_KEY', 'B2_TEST_APPLICATION_KEY_ID')


def triggers(workflow):
    return workflow.get('on', workflow.get(True))


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
    scripts = '\n'.join(
        step.get('run', '') for job in workflow['jobs'].values() for step in job.get('steps', [])
    )
    assert 'B2_TEST_ENVIRONMENT' in scripts
    assert 'uv run nox -vs integration' in scripts
    assert 'exit 1' in scripts


def test_live_b2_workflow_is_weekly_manual_and_fails_closed():
    text = LIVE_PATH.read_text()
    workflow = yaml.safe_load(text)
    assert set(triggers(workflow)) == {'schedule', 'workflow_dispatch'}
    assert triggers(workflow)['schedule']
    assert all(f'secrets.{key}' in text for key in KEYS)
    scripts = '\n'.join(
        step.get('run', '') for job in workflow['jobs'].values() for step in job.get('steps', [])
    )
    assert 'uv run nox -vs integration' in scripts
    assert 'exit 1' in scripts
