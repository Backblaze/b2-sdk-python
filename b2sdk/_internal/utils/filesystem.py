######################################################################
#
# File: b2sdk/_internal/utils/filesystem.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pathlib
import platform
import stat

_IS_WINDOWS = platform.system() == "Windows"


def points_to_fifo(path: pathlib.Path) -> bool:
    """Check if the path points to a fifo."""
    path = path.resolve()
    try:

        return stat.S_ISFIFO(path.stat().st_mode)
    except OSError:
        return False


_STDOUT_FILENAME = "CON" if _IS_WINDOWS else "/dev/stdout"
STDOUT_FILEPATH = pathlib.Path(_STDOUT_FILENAME)


def points_to_stdout(path: pathlib.Path) -> bool:
    """Check if the path points to stdout."""
    try:
        return path == STDOUT_FILEPATH or path.resolve() == STDOUT_FILEPATH
    except OSError:
        return False
