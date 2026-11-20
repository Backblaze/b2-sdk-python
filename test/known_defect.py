######################################################################
#
# File: test/known_defect.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import contextlib
import re

import pytest


class DefectStillPresent(AssertionError):
    """
    Raised by :func:`expect` when the behaviour that a ``@known_defect`` test describes is wrong.

    A test that has ordinary prerequisite assertions (setup that must hold whether or not the defect
    is fixed) marks itself ``raises=DefectStillPresent`` and checks the defect with :func:`expect`.
    A failed prerequisite is then a plain ``AssertionError`` and shows up as a real failure instead
    of being swallowed as the expected one.
    """


def expect(condition: object, message: str = '') -> None:
    """Assert the correct behaviour of a known defect; fails with :class:`DefectStillPresent`."""
    if not condition:
        raise DefectStillPresent(message)


@contextlib.contextmanager
def defect_raises(exc_type: type[BaseException], match: str | None = None):
    """
    Turn the one specific exception that a known defect produces into :class:`DefectStillPresent`.

    An exception of another type, or whose message does not match ``match`` (a regular expression
    searched in ``str(exception)``), propagates unchanged and is a real failure of the test.
    """
    try:
        yield
    except exc_type as exc:
        if match is not None and not re.search(match, str(exc)):
            raise
        raise DefectStillPresent(f'{type(exc).__name__}: {exc}') from exc


def known_defect(
    defect_id: str,
    summary: str,
    *,
    raises: type[BaseException] | tuple[type[BaseException], ...],
    condition: bool = True,
):
    """
    Mark a test that asserts the correct behaviour of a defect that is still present.

    The test is a strict expected failure (``xfail(strict=True)``), so the default test run stays
    green, and the run fails as soon as the defect is fixed ("XPASS(strict)"). When that happens,
    delete the ``@known_defect(...)`` line: the test then becomes a normal regression test.

    The tests carry the ``known_defect`` marker, so they can be run (and are reported as real
    failures) on demand with one command, for example::

        pytest --runxfail -m known_defect --api=v3 test/unit test/static

    :param defect_id: identifier of the defect, for example ``'C16'``
    :param summary: one-line description of the defect
    :param raises: the exception (or tuple of exceptions) the test raises while the defect is present.
                   Any other failure, in the test or in its setup, is reported as a real failure.
                   Use ``AssertionError`` for a test whose only assertion is the defect check,
                   :class:`DefectStillPresent` when the test also has prerequisite assertions, or
                   ``pytest.fail.Exception`` when the defect is "DID NOT RAISE" from ``pytest.raises``
    :param condition: when false the defect does not apply (for example to another apiver) and the
                      test is a normal test
    """

    def decorator(func):
        if not condition:
            return func
        func = pytest.mark.known_defect(func)
        return pytest.mark.xfail(
            strict=True, raises=raises, reason=f'Known defect {defect_id}: {summary}'
        )(func)

    return decorator
