######################################################################
#
# File: test/unit/test_simulator_encryption_context.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest

from b2sdk._internal.raw_simulator import FileSimulator

pytestmark = [pytest.mark.rawsimulator(reason='tests RawSimulator itself')]


@pytest.mark.parametrize('initial', [True, False])
def test_context_restores_previous_flag_after_exception(monkeypatch, initial):
    monkeypatch.setattr(FileSimulator, 'CHECK_ENCRYPTION', initial)
    with pytest.raises(RuntimeError, match='body failed'):
        with FileSimulator.dont_check_encryption():
            assert FileSimulator.CHECK_ENCRYPTION is False
            raise RuntimeError('body failed')
    assert FileSimulator.CHECK_ENCRYPTION is initial


def test_nested_context_restores_outer_flag_after_inner_exception(monkeypatch):
    monkeypatch.setattr(FileSimulator, 'CHECK_ENCRYPTION', True)
    with FileSimulator.dont_check_encryption():
        with pytest.raises(RuntimeError, match='inner failed'):
            with FileSimulator.dont_check_encryption():
                raise RuntimeError('inner failed')
        assert FileSimulator.CHECK_ENCRYPTION is False
        with FileSimulator.dont_check_encryption():
            assert FileSimulator.CHECK_ENCRYPTION is False
        assert FileSimulator.CHECK_ENCRYPTION is False
    assert FileSimulator.CHECK_ENCRYPTION is True
