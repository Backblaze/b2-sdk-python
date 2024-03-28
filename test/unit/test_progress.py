######################################################################
#
# File: test/unit/test_progress.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest
from apiver_deps import TqdmProgressListener, make_progress_listener


@pytest.mark.parametrize(
    "tqdm_available, quiet, expected_listener",
    [
        (True, False, "TqdmProgressListener"),
        (False, False, "SimpleProgressListener"),
        (False, True, "DoNothingProgressListener"),
    ],
)
def test_make_progress_listener(tqdm_available, quiet, expected_listener, monkeypatch):
    if not tqdm_available:
        monkeypatch.setattr("b2sdk.progress.tqdm", None)

    assert make_progress_listener("description", quiet).__class__.__name__ == expected_listener


def test_tqdm_progress_listener__without_tqdm_module(monkeypatch):
    monkeypatch.setattr("b2sdk.progress.tqdm", None)

    with pytest.raises(ModuleNotFoundError):
        TqdmProgressListener("description")
