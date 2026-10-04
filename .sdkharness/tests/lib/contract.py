"""Dispatcher for repository-owned sdkharness conformance and resilience checks."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
from loopback_guard import scrub_proxy_environment  # noqa: E402

SLUG = 'b2-sdk-python'
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_ROOT = Path(__file__).resolve().parents[1]
# The only COULD-NOT-RUN reasons that may become SKIP: the question genuinely
# could not be asked. At a simulator the harness supplies the server and the
# credential, so `unreachable`, `unauthorized` and an SDK import error are never
# legitimate ambers: any other reason is a FAIL (the CLI's SKIP policy).
SKIP_REASONS = frozenset({'no-realm-option', 'missing-runtime', 'no-client-option', 'not-claimed'})
SCENARIO_RE = re.compile(r'^[A-Za-z0-9_.-]+$')


class ContractFailure(Exception):
    """A credential-safe contract or execution failure."""


def result(level: str, scenario: str, outcome: str, reason: str) -> None:
    safe_reason = reason.replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')
    print(f'SDKHARNESS_RESULT\t{level}\t{scenario}\t{outcome}\t{safe_reason}')


def validate_loopback_origin(value: str) -> None:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise ContractFailure('configuration: invalid simulator URL') from error
    if (
        parsed.scheme != 'http'
        or parsed.hostname != '127.0.0.1'
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {'', '/'}
        or parsed.query
        or parsed.fragment
    ):
        raise ContractFailure('configuration: simulator URLs must be IPv4 loopback HTTP origins')


def validate_environment(level: str, environment: Mapping[str, str]) -> tuple[str, str]:
    if level not in {'conformance', 'resilience'}:
        raise ContractFailure('configuration: unsupported test level')
    if environment.get('SDKHARNESS_TEST_LEVEL') != level:
        raise ContractFailure('configuration: unexpected test level')
    scenario = environment.get('SDKHARNESS_SCENARIO', '')
    if not SCENARIO_RE.fullmatch(scenario):
        raise ContractFailure('configuration: invalid scenario')
    simulator_url = environment.get('SDKHARNESS_SIMULATOR_URL', '')
    validate_loopback_origin(simulator_url)
    if level == 'resilience':
        validate_loopback_origin(environment.get('SDKHARNESS_SIMULATOR_CONTROL_URL', ''))
    return scenario, simulator_url


def child_environment(level: str, environment: Mapping[str, str]) -> dict[str, str]:
    child = {name: value for name, value in environment.items() if not name.startswith('B2_')}
    scrub_proxy_environment(child)
    child['PYTHONPATH'] = str(REPOSITORY_ROOT)
    child['B2_APPLICATION_KEY_ID'] = 'test-key-id'
    child['B2_APPLICATION_KEY'] = 'test-key'
    child[f'{level.upper()}_TARGET'] = 'simulator'
    child[f'{level.upper()}_SIMULATOR_URL'] = environment['SDKHARNESS_SIMULATOR_URL']
    child[f'{level.upper()}_SIMULATOR_HTTPS_URL'] = environment.get(
        'SDKHARNESS_SIMULATOR_HTTPS_URL', ''
    )
    child[f'{level.upper()}_SIMULATOR_CA'] = environment.get('SDKHARNESS_SIMULATOR_CA', '')
    if level == 'resilience':
        child['RESILIENCE_CONTROL_URL'] = environment['SDKHARNESS_SIMULATOR_CONTROL_URL']
    return child


def translate(level: str, scenario: str, returncode: int, output: str) -> tuple[str, str, int]:
    prefix = f'{level.upper()} {SLUG} {scenario} @'
    standing = [line for line in output.splitlines() if line.startswith(prefix)]
    for line in output.splitlines():
        if not line.startswith(prefix):
            print(line)
    if len(standing) != 1:
        return 'FAIL', f'contract: expected one standing result, got {len(standing)}', 1
    try:
        verdict = standing[0].split(': ', 1)[1]
    except IndexError:
        return 'FAIL', 'contract: malformed standing verdict', 1
    if verdict == 'PASS':
        if returncode != 0:
            return 'FAIL', f'contract: PASS exited {returncode}', 1
        return 'PASS', '-', 0
    if verdict.startswith('FAIL (') and verdict.endswith(')'):
        return 'FAIL', verdict[6:-1], 1
    if verdict.startswith('COULD-NOT-RUN (') and verdict.endswith(')'):
        if returncode != 0:
            return 'FAIL', f'contract: SKIP exited {returncode}', 1
        reason = verdict[15:-1]
        if reason.split(' -- ', 1)[0] not in SKIP_REASONS:
            return 'FAIL', f'contract: {reason}', 1
        return 'SKIP', reason, 0
    return 'FAIL', 'contract: malformed standing verdict', 1


def run(level: str, environment: Mapping[str, str] = os.environ) -> int:
    scenario = environment.get('SDKHARNESS_SCENARIO', 'unknown')
    try:
        scenario, _ = validate_environment(level, environment)
        executable = SCENARIO_ROOT / level / scenario
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise ContractFailure('configuration: scenario executable is unavailable')
        completed = subprocess.run(
            [str(executable)],
            cwd=REPOSITORY_ROOT,
            env=child_environment(level, environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        outcome, reason, returncode = translate(
            level, scenario, completed.returncode, completed.stdout
        )
    except ContractFailure as error:
        outcome, reason, returncode = 'FAIL', str(error), 1
    except Exception as error:
        outcome, reason, returncode = 'FAIL', f'setup: {type(error).__name__}', 1
    result(level, scenario, outcome, reason)
    return returncode


def main(level: str) -> int:
    return run(level)


if __name__ == '__main__':
    sys.exit(2)
