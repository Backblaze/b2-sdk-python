######################################################################
#
# File: test/static/test_ci_draft_safety.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Exercise CI's draft, credential and dependency gates without running jobs."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

JOBS = yaml.safe_load((Path(__file__).parents[2] / '.github/workflows/ci.yml').read_text())['jobs']


def expression(value, context):
    """Evaluate only the boolean/string expression subset used by these gates.

    actionlint validates the complete workflow separately. Unknown contexts or
    expression syntax fail this check rather than being assumed safe.
    """
    if not isinstance(value, str) or not value.startswith('${{'):
        return value
    code = value[3:-2].strip().replace('cancelled()', repr(context['cancelled']))
    code = re.sub(
        r'[a-zA-Z_][\w-]*(?:\.[a-zA-Z_][\w-]*)+', lambda match: repr(context[match.group()]), code
    )
    code = code.replace('&&', ' and ').replace('||', ' or ')
    code = re.sub(r'!(?!=)', 'not ', code)
    code = re.sub(r'\btrue\b', 'True', code)
    code = re.sub(r'\bfalse\b', 'False', code)
    tree = ast.parse(code.strip(), mode='eval')
    allowed = (
        ast.Expression,
        ast.Constant,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.UnaryOp,
        ast.Not,
        ast.Compare,
        ast.Eq,
        ast.NotEq,
    )
    assert all(isinstance(node, allowed) for node in ast.walk(tree)), code
    return eval(compile(tree, '<workflow-condition>', 'eval'), {'__builtins__': {}}, {})


def context(draft=True, credentials=True, event='pull_request', lint='success', cleanup='skipped'):
    return {
        'github.event_name': event,
        'github.event.pull_request.draft': draft,
        'needs.lint.result': lint,
        'needs.cleanup_buckets.result': cleanup,
        'cancelled': False,
        'secrets.B2_TEST_APPLICATION_KEY': 'test-key' if credentials else '',
        'secrets.B2_TEST_APPLICATION_KEY_ID': 'test-id' if credentials else '',
    }


@pytest.mark.parametrize('credentials', [True, False])
def test_draft_never_starts_bucket_cleanup(credentials):
    assert not expression(
        JOBS['cleanup_buckets'].get('if', '${{ true }}'), context(credentials=credentials)
    )


@pytest.mark.parametrize('job_id', ['test', 'cli-compat'])
@pytest.mark.parametrize(
    'draft,event', [(True, 'pull_request'), (False, 'pull_request'), (False, 'push')]
)
def test_b2_credentials_are_empty_only_for_drafts(job_id, draft, event):
    values = context(draft=draft, event=event)
    for name in ('B2_TEST_APPLICATION_KEY', 'B2_TEST_APPLICATION_KEY_ID'):
        actual = expression(JOBS[job_id]['env'][name], values)
        assert actual == ('' if draft else values[f'secrets.{name}'])


@pytest.mark.parametrize('job_id', ['test', 'cli-compat'])
@pytest.mark.parametrize(
    'draft,credentials,allowed',
    [(True, True, False), (True, False, False), (False, True, True), (False, False, False)],
)
def test_live_steps_require_non_draft_and_credentials(job_id, draft, credentials, allowed):
    values = context(draft=draft, credentials=credentials)
    values.update(
        {
            f'env.{name}': expression(value, values)
            for name, value in JOBS[job_id]['env'].items()
            if name.startswith('B2_TEST_')
        }
    )
    values.update({'matrix.os': 'ubuntu-latest', 'matrix.python-version': '3.14'})
    steps = [
        step
        for step in JOBS[job_id]['steps']
        if step.get('name') in ('Run integration tests', 'Run CLI integration tests (with secrets)')
    ]
    assert len(steps) == 1
    assert bool(expression(steps[0]['if'], values)) == allowed


@pytest.mark.parametrize('job_id', ['test', 'cli-compat'])
@pytest.mark.parametrize(
    'lint,cleanup,allowed',
    [
        ('success', 'skipped', True),
        ('success', 'success', True),
        ('failure', 'skipped', False),
        ('success', 'failure', False),
        ('success', 'cancelled', False),
    ],
)
def test_unit_jobs_survive_skipped_cleanup_but_not_failed_dependencies(
    job_id, lint, cleanup, allowed
):
    assert 'lint' in JOBS[job_id]['needs']
    assert 'cleanup_buckets' in JOBS[job_id]['needs']
    values = context(lint=lint, cleanup=cleanup)
    assert bool(expression(JOBS[job_id]['if'], values)) == allowed
    values['cancelled'] = True
    assert not expression(JOBS[job_id]['if'], values)


@pytest.mark.parametrize('event', ['pull_request', 'push'])
def test_non_draft_cleanup_is_retained(event):
    assert expression(JOBS['cleanup_buckets']['if'], context(draft=False, event=event))
