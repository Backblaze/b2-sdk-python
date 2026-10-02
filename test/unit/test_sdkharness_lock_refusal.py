######################################################################
#
# File: test/unit/test_sdkharness_lock_refusal.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

import pytest

CONFORMANCE = Path(__file__).parents[2] / '.sdkharness' / 'tests' / 'conformance'
LEAVES = ('lock.bypass_governance', 'lock.per_file_retention')
EXPECTED = ('AccessDenied', 'RetentionWriteError', 'Conflict', 'BadRequest')


def load_leaf(name):
    loader = SourceFileLoader(f'sdkharness_leaf_{name.replace(".", "_")}', str(CONFORMANCE / name))
    spec = spec_from_loader(loader.name, loader)
    module = module_from_spec(spec)
    loader.exec_module(module)
    return module


# `refused` classifies by class name, as the leaves do, so these stand-ins carry
# b2sdk's names. Unauthorized carries the `code` b2sdk reads from the response.
class Unauthorized(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


class InvalidAuthToken(Unauthorized):
    pass


class ServiceError(Exception):
    pass


def raising(error):
    def action():
        raise error

    return action


@pytest.mark.parametrize('leaf', LEAVES)
def test_401_access_denied_counts_as_the_refusal(leaf):
    module = load_leaf(leaf)
    got = module.refused(
        'delete without bypass',
        raising(Unauthorized('access_denied')),
        EXPECTED,
        access_denied_401=True,
    )
    assert got == 'Unauthorized(access_denied)'


@pytest.mark.parametrize('leaf', LEAVES)
def test_the_old_refusal_classes_are_still_accepted(leaf):
    module = load_leaf(leaf)
    AccessDenied = type('AccessDenied', (Exception,), {})
    assert (
        module.refused(
            'delete without bypass', raising(AccessDenied()), EXPECTED, access_denied_401=True
        )
        == 'AccessDenied'
    )


@pytest.mark.parametrize('leaf', LEAVES)
def test_an_allowed_operation_still_fails(leaf):
    module = load_leaf(leaf)
    with pytest.raises(module.Failure, match='permitted'):
        module.refused('delete without bypass', lambda: None, EXPECTED, access_denied_401=True)


@pytest.mark.parametrize('leaf', LEAVES)
@pytest.mark.parametrize(
    'error',
    [
        InvalidAuthToken('expired_auth_token'),
        Unauthorized('bad_auth_token'),
        Unauthorized('email_not_verified'),
        ServiceError('503'),
        KeyError('404'),
    ],
    ids=['bad-token', 'other-401-code', 'unverified-email', '5xx', 'not-found'],
)
def test_an_unrelated_refusal_still_fails(leaf, error):
    module = load_leaf(leaf)
    with pytest.raises(module.Failure, match='refused with'):
        module.refused('delete without bypass', raising(error), EXPECTED, access_denied_401=True)


@pytest.mark.parametrize('leaf', LEAVES)
def test_unauthorized_is_not_accepted_unless_asked(leaf):
    module = load_leaf(leaf)
    with pytest.raises(module.Failure):
        module.refused('delete without bypass', raising(Unauthorized('access_denied')), EXPECTED)


@pytest.mark.parametrize('leaf', LEAVES)
def test_the_leaf_asks_for_the_401_on_its_delete_without_bypass(leaf):
    source = (CONFORMANCE / leaf).read_text()
    assert 'access_denied_401=True' in source
