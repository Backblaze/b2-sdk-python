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

import pytest


def known_defect(defect_id: str, summary: str, *, condition: bool = True):
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
    :param condition: when false the defect does not apply (for example to another apiver) and the
                      test is a normal test
    """

    def decorator(func):
        if not condition:
            return func
        func = pytest.mark.known_defect(func)
        return pytest.mark.xfail(strict=True, reason=f'Known defect {defect_id}: {summary}')(func)

    return decorator
