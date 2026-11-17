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
import pathlib
import platform

import nox

CI = os.environ.get('CI') is not None
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')
_NOX_EXTRAS = os.environ.get('NOX_EXTRAS')
NOX_EXTRAS = [[]] if _NOX_EXTRAS is None else list(filter(None, [_NOX_EXTRAS.split(',')]))

PYTHON_VERSIONS = (
    [
        'pypy3.10',
        '3.10',
        '3.11',
        '3.12',
        '3.13',
        '3.14',
    ]
    if NOX_PYTHONS is None
    else NOX_PYTHONS.split(',')
)


def _detect_python_nox_id() -> str:
    major, minor, *_ = platform.python_version_tuple()
    python_nox_id = f'{major}.{minor}'
    if platform.python_implementation() == 'PyPy':
        python_nox_id = f'pypy{python_nox_id}'
    return python_nox_id


if CI and not NOX_PYTHONS:
    # this is done to allow it to work even if `nox -p` was passed to nox
    PYTHON_VERSIONS = [_detect_python_nox_id()]
    print(f'CI job mode; using provided interpreter only; PYTHON_VERSIONS={PYTHON_VERSIONS!r}')

PYTHON_DEFAULT_VERSION = next(
    (version for version in reversed(PYTHON_VERSIONS) if not version.lower().startswith('pypy')),
    PYTHON_VERSIONS[-1],
)

PY_PATHS = ['b2sdk', 'test', 'scripts', 'noxfile.py']

nox.options.default_venv_backend = 'uv'
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'lint',
    'test',
]


def uv_install(
    session: nox.Session, *, groups: tuple[str, ...] = (), extras: tuple[str, ...] = ()
) -> None:
    args = ['sync', '--locked']
    for group in groups:
        if group:
            args.extend(['--group', group])
    for extra in extras:
        if extra:
            args.extend(['--extra', extra])
    uv_env = getattr(session.virtualenv, 'location', os.getenv('VIRTUAL_ENV'))

    session.run_install('uv', *args, env={'UV_PROJECT_ENVIRONMENT': uv_env})


def skip_coverage(python_version: str | None) -> bool:
    return (python_version or platform.python_implementation()).lower().startswith('pypy')


@nox.session(name='format', python=PYTHON_DEFAULT_VERSION)
def format_(session):
    """Lint the code and apply fixes in-place whenever possible."""
    uv_install(session, groups=('format',))
    session.run('ruff', 'check', '--fix', *PY_PATHS)
    session.run('ruff', 'format', *PY_PATHS)
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
    # We need to install 'doc' group because liccheck needs to inspect it.
    uv_install(session, groups=('doc', 'lint'), extras=('full',))
    session.run('ruff', 'check', *PY_PATHS)
    session.run('ruff', 'format', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--check',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )

    session.run('pytest', 'test/static')
    session.run('liccheck', '-s', 'pyproject.toml')


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize('extras', NOX_EXTRAS)
def unit(session, extras):
    """Run unit tests."""
    uv_install(session, groups=('test',), extras=tuple(extras))
    args = ['--doctest-modules', '-n', 'auto']
    if not skip_coverage(session.python):
        args += ['--cov=b2sdk', '--cov-branch', '--cov-report=xml']
    # TODO: Use session.parametrize for apiver
    session.run('pytest', '--api=v3', *args, *session.posargs, 'test/unit')
    if not skip_coverage(session.python):
        args += ['--cov-append']
    session.run('pytest', '--api=v2', *args, *session.posargs, 'test/unit')
    session.run('pytest', '--api=v1', *args, *session.posargs, 'test/unit')
    session.run('pytest', '--api=v0', *args, *session.posargs, 'test/unit')

    if not skip_coverage(session.python) and not session.posargs:
        session.notify('cover')


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize('extras', NOX_EXTRAS)
def integration(session, extras):
    """Run integration tests."""
    uv_install(session, groups=('test',), extras=tuple(extras))
    session.run('pytest', '-s', *session.posargs, 'test/integration')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def cleanup_old_buckets(session):
    """Remove buckets from previous test runs."""
    uv_install(session, groups=('test',))
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
    uv_install(session, groups=('test',))
    session.run('coverage', 'report', '--fail-under=75', '--show-missing', '--skip-covered')
    session.run('coverage', 'erase')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build(session):
    """Build the distribution."""
    session.run('rm', '-rf', 'build', 'dist', external=True)
    session.run('uv', 'build', external=True)

    # Set outputs for GitHub Actions
    if CI:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as github_output:
            # Path have to be specified with unix style slashes even for windows,
            # otherwise glob won't find files on windows in action-gh-release.
            print('asset_path=dist/*', file=github_output)

            version = os.environ['GITHUB_REF'].replace('refs/tags/v', '')
            print(f'version={version}', file=github_output)

    sdists = sorted(pathlib.Path('dist').glob('b2sdk-*.tar.gz'))
    if len(sdists) != 1:
        session.error(f'Expected exactly one source distribution, found {len(sdists)}: {sdists!r}')

    session.install('pip>=26.2.2', str(sdists[0]))
    session.cd('dist')  # avoid importing from the checkout instead of the built package
    session.run(
        'python',
        '-c',
        (
            'import pathlib; '
            'from b2sdk import v0, v1, v2, v3; '
            'repo_root = pathlib.Path.cwd().parent.resolve(); '
            'source_root = repo_root / "b2sdk"; '
            "module_files = {'v0': pathlib.Path(v0.__file__).resolve(), "
            "'v1': pathlib.Path(v1.__file__).resolve(), "
            "'v2': pathlib.Path(v2.__file__).resolve(), "
            "'v3': pathlib.Path(v3.__file__).resolve()}; "
            'print(module_files); '
            'assert all(not path.is_relative_to(source_root) for path in module_files.values()), '
            "f'Imported modules from checkout: {module_files!r}'; "
            "assert all('site-packages' in path.parts for path in module_files.values()), "
            "f'Imported modules from an unexpected location: {module_files!r}'"
        ),
    )


@nox.session(python=PYTHON_DEFAULT_VERSION)
def doc(session):
    """Build the documentation."""
    uv_install(session, groups=('doc',))
    session.cd('doc')
    sphinx_args = ['-b', 'html', '-T', '-W', 'source', 'build/html']
    session.run('rm', '-rf', 'build', external=True)

    if not session.interactive:
        session.run('sphinx-build', *sphinx_args)
        session.notify('doc_cover')
    else:
        sphinx_args[-2:-2] = [
            '-E',
            '--open-browser',
            '--watch',
            '../b2sdk',
            '--ignore',
            '*.pyc',
            '--ignore',
            '*~',
        ]
        session.run('sphinx-autobuild', *sphinx_args)


@nox.session
def doc_cover(session):
    """Perform coverage analysis for the documentation."""
    uv_install(session, groups=('doc',))
    session.cd('doc')
    sphinx_args = ['-b', 'coverage', '-T', '-W', 'source', 'build/coverage']
    report_file = 'build/coverage/python.txt'
    session.run('sphinx-build', *sphinx_args)
    session.run('cat', report_file, external=True)

    with open('build/coverage/python.txt') as fd:
        # If there is no undocumented files, the report should have only 3 lines (header)
        if sum(1 for _ in fd) != 3:
            session.error('sphinx coverage has failed')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def make_release_commit(session):
    """Prepare a Changie batch on clean master; never commit, tag or publish."""
    if len(session.posargs) != 1:
        session.error('Provide -- X.Y.Z (without leading v)')
    session.run('python', 'scripts/prepare_release.py', session.posargs[0])


@nox.session(python=PYTHON_DEFAULT_VERSION)
def changelog_check(session):
    """Validate Changie fragments, generated history and optional PR coverage."""
    uv_install(session, groups=('release',))
    session.run('python', 'scripts/check_changelog.py', *session.posargs)
