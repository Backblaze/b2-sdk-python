"""Refuse to run a repository-owned leaf check outside a loopback simulator.

The dispatchers (`run-conformance`, `run-resilience`) already validate their
invocation. The leaf checks are executable on their own, so each one calls
`refusal()` first: a leaf that is run directly can only ever talk to a loopback
simulator with the fixed test credential, never to staging or production with
ambient `B2_*` credentials.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from urllib.parse import urlsplit

PROXY_VARIABLES = frozenset(
    {'http_proxy', 'https_proxy', 'all_proxy', 'ftp_proxy', 'no_proxy', 'request_method'}
)


def scrub_proxy_environment(environment: MutableMapping[str, str]) -> None:
    """Remove proxy settings so a stray proxy cannot change a loopback result.

    `requests` and `urllib` honor `HTTP_PROXY`, `HTTPS_PROXY` and `ALL_PROXY`
    (any case) even for 127.0.0.1, which turns a passing scenario into an
    "unreachable" one on a machine that has a proxy configured. The proxy
    variables are dropped and `NO_PROXY` is pinned to the loopback address, so
    nothing but the simulator is ever reached directly and nothing is proxied.
    `REQUEST_METHOD` (a CGI marker that makes `requests` ignore `HTTP_PROXY`)
    is dropped too so the behavior does not depend on it.
    """
    for name in list(environment):
        if name.lower() in PROXY_VARIABLES:
            del environment[name]
    environment['NO_PROXY'] = '127.0.0.1'
    environment['no_proxy'] = '127.0.0.1'


def missing_runtime(error: ImportError) -> bool:
    """True only when `b2sdk` itself is not installed (no such top-level module).

    Any other ImportError means b2sdk is installed but broken (a failed or
    circular import inside the SDK): that is an SDK regression and a FAIL.
    """
    return isinstance(error, ModuleNotFoundError) and error.name == 'b2sdk'


def _is_loopback_origin(value: str, scheme: str) -> bool:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == scheme
        and parsed.hostname == '127.0.0.1'
        and port is not None
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {'', '/'}
        and not parsed.query
        and not parsed.fragment
    )


def refusal(level: str) -> str | None:
    """The reason this leaf must not run here, or None when it is safe to run.

    `level` is `conformance` or `resilience`. The environment is the one the
    dispatcher builds: `<LEVEL>_TARGET`, `<LEVEL>_SIMULATOR_URL`, optionally
    `<LEVEL>_SIMULATOR_HTTPS_URL`, and `RESILIENCE_CONTROL_URL` for resilience.
    """
    scrub_proxy_environment(os.environ)
    prefix = level.upper()
    target = os.environ.get(f'{prefix}_TARGET', 'simulator')
    if target != 'simulator':
        return f'configuration -- only the simulator target is supported, got {target!r}'
    hint = 'run it through .sdkharness/tests/run-' + level + ' with a loopback simulator'
    if not _is_loopback_origin(os.environ.get(f'{prefix}_SIMULATOR_URL', ''), 'http'):
        return f'configuration -- {prefix}_SIMULATOR_URL must be http://127.0.0.1:<port>; {hint}'
    https_url = os.environ.get(f'{prefix}_SIMULATOR_HTTPS_URL', '')
    if https_url and not _is_loopback_origin(https_url, 'https'):
        return f'configuration -- {prefix}_SIMULATOR_HTTPS_URL must be https://127.0.0.1:<port>'
    if level == 'resilience' and not _is_loopback_origin(
        os.environ.get('RESILIENCE_CONTROL_URL', ''), 'http'
    ):
        return 'configuration -- RESILIENCE_CONTROL_URL must be http://127.0.0.1:<port>'
    return None
