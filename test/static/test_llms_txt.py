######################################################################
#
# File: test/static/test_llms_txt.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PREFIX = 'https://github.com/Backblaze/b2-sdk-python/blob/master/'


def check_repo_links(text, root):
    for line_number, line in enumerate(text.splitlines(), 1):
        for path in re.findall(re.escape(PREFIX) + r'([^\s)#]+)', line):
            assert (root / path).is_file(), f'line {line_number}: missing {path}'


def test_llms_txt_repo_links():
    check_repo_links((ROOT / 'llms.txt').read_text(encoding='utf-8'), ROOT)


def test_missing_repo_link_is_rejected(tmp_path):
    with pytest.raises(AssertionError, match='line 1: missing missing.md'):
        check_repo_links(f'[missing]({PREFIX}missing.md)', tmp_path)
