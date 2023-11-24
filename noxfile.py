######################################################################
#
# File: noxfile.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os
import re
import subprocess

import nox

CI = os.environ.get('CI') is not None
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')
SKIP_COVERAGE = os.environ.get('SKIP_COVERAGE') == 'true'

PYTHON_VERSIONS = [
    '3.7',
    '3.8',
    '3.9',
    '3.10',
    '3.11',
    '3.12',
] if NOX_PYTHONS is None else NOX_PYTHONS.split(',')

PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

PY_PATHS = ['b2sdk', 'test', 'noxfile.py', 'setup.py']

REQUIREMENTS_FORMAT = ['yapf==0.27', 'ruff==0.0.270']
REQUIREMENTS_LINT = REQUIREMENTS_FORMAT + ['pytest==6.2.5', 'liccheck==0.6.2']
REQUIREMENTS_RELEASE = ['towncrier==23.11.0']
REQUIREMENTS_TEST = [
    "pytest==6.2.5",
    "pytest-cov==3.0.0",
    "pytest-mock==3.6.1",
    'pytest-lazy-fixture==0.6.3',
    'pytest-xdist==2.5.0',
    'pytest-timeout==2.1.0',
]
REQUIREMENTS_BUILD = ['setuptools>=20.2', 'wheel>=0.40']

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'lint',
    'test',
]

# In CI, use Python interpreter provided by GitHub Actions
if CI:
    nox.options.force_venv_backend = 'none'


def install_myself(session, extras=None):
    """Install from the source."""
    arg = '.'
    if extras:
        arg += '[%s]' % ','.join(extras)

    session.run('pip', 'install', '-e', arg)


@nox.session(name='format', python=PYTHON_DEFAULT_VERSION)
def format_(session):
    """Lint the code and apply fixes in-place whenever possible."""
    session.run('pip', 'install', *REQUIREMENTS_FORMAT)
    # TODO: incremental mode for yapf
    session.run('yapf', '--in-place', '--parallel', '--recursive', *PY_PATHS)
    session.run('ruff', 'check', '--fix', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--in-place',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )


@nox.session(python=PYTHON_DEFAULT_VERSION)
def lint(session):
    """Run linters in readonly mode."""
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_LINT)
    session.run('yapf', '--diff', '--parallel', '--recursive', *PY_PATHS)
    session.run('ruff', 'check', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--check',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )

    session.run('pytest', 'test/static')
    session.run('liccheck', '-s', 'setup.cfg')


@nox.session(python=PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    args = ['--doctest-modules', '-n', 'auto']
    if not SKIP_COVERAGE:
        args += ['--cov=b2sdk', '--cov-branch', '--cov-report=xml']
    # TODO: Use session.parametrize for apiver
    session.run('pytest', '--api=v3', *args, *session.posargs, 'test/unit')
    if not SKIP_COVERAGE:
        args += ['--cov-append']
    session.run('pytest', '--api=v2', *args, *session.posargs, 'test/unit')
    session.run('pytest', '--api=v1', *args, *session.posargs, 'test/unit')
    session.run('pytest', '--api=v0', *args, *session.posargs, 'test/unit')

    if not SKIP_COVERAGE and not session.posargs:
        session.notify('cover')


@nox.session(python=PYTHON_VERSIONS)
def integration(session):
    """Run integration tests."""
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    session.run('pytest', '-s', *session.posargs, 'test/integration')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def cleanup_old_buckets(session):
    """Remove buckets from previous test runs."""
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    session.run('python', '-m', 'test.integration.cleanup_buckets')


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    """Run all tests."""
    if session.python:
        session.notify(f'unit-{session.python}')
        session.notify(f'integration-{session.python}')
    else:
        session.notify('unit')
        session.notify('integration')


@nox.session
def cover(session):
    """Perform coverage analysis."""
    session.run('pip', 'install', 'coverage')
    session.run('coverage', 'report', '--fail-under=75', '--show-missing', '--skip-covered')
    session.run('coverage', 'erase')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build(session):
    """Build the distribution."""
    session.run('pip', 'install', *REQUIREMENTS_BUILD)
    session.run('python', 'setup.py', 'check', '--metadata', '--strict')
    session.run('rm', '-rf', 'build', 'dist', 'b2sdk.egg-info', external=True)
    session.run('python', 'setup.py', 'sdist', *session.posargs)
    session.run('python', 'setup.py', 'bdist_wheel', *session.posargs)

    # Set outputs for GitHub Actions
    if CI:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as github_output:
            # Path have to be specified with unix style slashes even for windows,
            # otherwise glob won't find files on windows in action-gh-release.
            print('asset_path=dist/*', file=github_output)

            version = os.environ['GITHUB_REF'].replace('refs/tags/v', '')
            print(f'version={version}', file=github_output)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def doc(session):
    """Build the documentation."""
    install_myself(session, extras=['doc'])
    session.cd('doc')
    sphinx_args = ['-b', 'html', '-T', '-W', 'source', 'build/html']
    session.run('rm', '-rf', 'build', external=True)

    if not session.interactive:
        session.run('sphinx-build', *sphinx_args)
        session.notify('doc_cover')
    else:
        sphinx_args[-2:-2] = [
            '-E', '--open-browser', '--watch', '../b2sdk', '--ignore', '*.pyc', '--ignore', '*~'
        ]
        session.run('sphinx-autobuild', *sphinx_args)


@nox.session
def doc_cover(session):
    """Perform coverage analysis for the documentation."""
    install_myself(session, extras=['doc'])
    session.cd('doc')
    sphinx_args = ['-b', 'coverage', '-T', '-W', 'source', 'build/coverage']
    report_file = 'build/coverage/python.txt'
    session.run('sphinx-build', *sphinx_args)
    session.run('cat', report_file, external=True)

    with open('build/coverage/python.txt') as fd:
        # If there is no undocumented files, the report should have only 2 lines (header)
        if sum(1 for _ in fd) != 2:
            session.error('sphinx coverage has failed')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def make_release_commit(session):
    """
    Runs `towncrier build`, commits changes, tags, all that is left to do is pushing
    """
    if session.posargs:
        version = session.posargs[0]
    else:
        session.error('Provide -- {release_version} (X.Y.Z - without leading "v")')

    if not re.match(r'^\d+\.\d+\.\d+$', version):
        session.error(
            f'Provided version="{version}". Version must be of the form X.Y.Z where '
            f'X, Y and Z are integers'
        )

    local_changes = subprocess.check_output(['git', 'diff', '--stat'])
    if local_changes:
        session.error('Uncommitted changes detected')

    current_branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode()
    if current_branch != 'master':
        session.log('WARNING: releasing from a branch different than master')

    session.run('pip', 'install', *REQUIREMENTS_RELEASE)
    session.run('towncrier', 'build', '--yes', '--version', version)

    session.log(
        f'CHANGELOG updated, changes ready to commit and push\n'
        f'    git commit -m release {version}\n'
        f'    git tag v{version}\n'
        f'    git push {{UPSTREAM_NAME}} v{version}\n'
        f'    git push {{UPSTREAM_NAME}} {current_branch}'
    )
