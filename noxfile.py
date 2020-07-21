import os
import subprocess

import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'liccheck',
    'lint',
    'test',
]

PY_PATHS = ['b2sdk', 'test', 'noxfile.py', 'setup.py']
CI = os.environ.get('CI') is not None
PYTHON_VERSIONS = ['3.5', '3.6', '3.7', '3.8']
# TODO: remove nose and pyflakes
REQUIREMENTS = {
    'format': ['docformatter==1.3.1', 'isort==5.1.1', 'yapf==0.27'],
    'lint': ['pyflakes', 'flake8==3.8.3'],
    'test': ['nose==1.3.7', 'pytest==5.4.3', 'pytest-cov==2.10.0'],
}

#
# DEVELOPMENT
#


# noinspection PyShadowingBuiltins
@nox.session(python=PYTHON_VERSIONS[-1])
def format(session):
    """Run code formatters."""
    session.install(*REQUIREMENTS['format'])
    # TODO: incremental mode for yapf
    session.run('yapf', '--in-place', '--parallel', '--recursive', *PY_PATHS)
    # TODO: uncomment if we want to use isort and docformatter
    # session.run('isort', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--in-place',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )


@nox.session(python=PYTHON_VERSIONS[-1])
def lint(session):
    """Run linters."""
    session.install(*REQUIREMENTS['format'], *REQUIREMENTS['lint'])
    session.run('python', 'setup.py', 'check', '--metadata', '--strict', silent=True)
    session.run('yapf', '--diff', '--parallel', '--recursive', *PY_PATHS)
    # TODO: uncomment if we want to use isort and docformatter
    # session.run('isort', '--check', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--check',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )

    # TODO: use flake8 instead of pyflakes
    session.log('pyflakes b2sdk')
    output = subprocess.run('pyflakes b2sdk', shell=True, check=False,
                            stdout=subprocess.PIPE).stdout.decode().strip()
    excludes = ['__init__.py', 'exception.py']
    output = [l for l in output.splitlines() if all(x not in l for x in excludes)]
    if output:
        print('\n'.join(output))
        session.error('pyflakes has failed')
    # session.run('flake8', *PY_PATHS)


@nox.session(python=PYTHON_VERSIONS[-1])
def liccheck(session):
    """Run license check."""
    session.install('-e', '.', 'liccheck==0.4.7')
    session.run('liccheck', '-s', 'setup.cfg')

#
# TESTING
#


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    """Run unit tests."""
    session.install('-e', '.')
    tests = session.posargs or ['test']
    session.run('pytest', '--cov=b2sdk', '--cov-report=', '--doctest-modules', *tests)
    session.notify('cover')


@nox.session
def cover(session):
    """Perform coverage analysis."""
    session.install('coverage')
    session.run('coverage', 'report', '--fail-under=77', '--show-missing')
    session.run('coverage', 'erase')


#
# DEPLOYMENT
#


@nox.session(python=PYTHON_VERSIONS[-1])
def build(session):
    """Build a distribution."""
    # TODO: consider using wheel as well
    session.install('setuptools')
    session.run('rm', '-rf', 'build', 'dist', 'b2sdk.egg-info', external=True)
    session.run('python', 'setup.py', 'sdist', *session.posargs)


@nox.session(python=PYTHON_VERSIONS[-1])
def deploy(session):
    """Deploy the distribution to the PYPI."""
    session.install('twine')
    session.run('twine', 'upload', 'dist/*')
