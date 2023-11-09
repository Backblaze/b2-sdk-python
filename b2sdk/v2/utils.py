######################################################################
#
# File: b2sdk/v2/utils.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import annotations

import shutil
import tempfile
import warnings


class TempDir:
    """
    Context manager that creates and destroys a temporary directory.
    """

    def __enter__(self):
        """
        Return the unicode path to the temp dir.
        """
        warnings.warn(
            'TempDir is deprecated. Use tempfile.TemporaryDirectory or pytest tmp_path fixture instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        dirpath_bytes = tempfile.mkdtemp()
        self.dirpath = str(dirpath_bytes.replace('\\', '\\\\'))
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.dirpath)
        return None  # do not hide exception
