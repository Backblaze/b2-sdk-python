######################################################################
#
# File: test/static/test_release_workflow_safety.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Check the CI permissions, action pins, and release test gate."""

import re
from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).parents[2] / '.github' / 'workflows'


def load_workflow(name):
    return yaml.safe_load((WORKFLOWS / name).read_text())


def test_ci_token_is_read_only():
    assert load_workflow('ci.yml')['permissions'] == {'contents': 'read'}


def test_active_actions_are_pinned_to_full_commit_shas():
    for path in WORKFLOWS.glob('*.yml'):
        for job in load_workflow(path)['jobs'].values():
            for step in job.get('steps', []):
                action = step.get('uses', '')
                if action and not action.startswith(('./', 'docker://')):
                    assert re.fullmatch(r'[^@]+@[0-9a-f]{40}', action), action


def test_release_requires_successful_unit_tests():
    jobs = load_workflow('cd.yml')['jobs']
    assert jobs['deploy']['needs'] == 'test'
    assert 'if' not in jobs['deploy']
    steps = jobs['test']['steps']
    assert any('nox -vs unit' in step.get('run', '') for step in steps)
    assert jobs['test'].get('permissions') == {'contents': 'read'}
