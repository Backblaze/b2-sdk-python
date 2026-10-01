"""Refuse to run a repository-owned leaf check outside a loopback simulator.

The dispatchers (`run-conformance`, `run-resilience`) already validate their
invocation. The leaf checks are executable on their own, so each one calls
`refusal()` first: a leaf that is run directly can only ever talk to a loopback
simulator with the fixed test credential, never to staging or production with
ambient `B2_*` credentials.
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit


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
