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
import re
import stat

_IS_WINDOWS = platform.system() == 'Windows'

DRIVE_MATCHER = re.compile(r'^([A-Za-z]):([/\\])')
ABSOLUTE_PATH_MATCHER = re.compile(r'^(/)|^(\\)')
RELATIVE_PATH_MATCHER = re.compile(
    # "abc" and "xyz" represent anything, including "nothing"
    r'^(\.\.[/\\])|'  # ../abc or ..\abc
    + r'^(\.[/\\])|'  # ./abc or .\abc
    + r'([/\\]\.\.[/\\])|'  # abc/../xyz or abc\..\xyz or abc\../xyz or abc/..\xyz
    + r'([/\\]\.[/\\])|'  # abc/./xyz or abc\.\xyz or abc\./xyz or abc/.\xyz
    + r'([/\\]\.\.)$|'  # abc/.. or abc\..
    + r'([/\\]\.)$|'  # abc/. or abc\.
    + r'^(\.\.)$|'  # just ".."
    + r'([/\\][/\\])|'  # abc\/xyz or abc/\xyz or abc//xyz or abc\\xyz
    + r'^(\.)$'  # just "."
)


def points_to_fifo(path: pathlib.Path) -> bool:
    """Check if the path points to a fifo."""
    path = path.resolve()
    try:
        return stat.S_ISFIFO(path.stat().st_mode)
    except OSError:
        return False


_STDOUT_FILENAME = 'CON' if _IS_WINDOWS else '/dev/stdout'
STDOUT_FILEPATH = pathlib.Path(_STDOUT_FILENAME)


def points_to_stdout(path: pathlib.Path) -> bool:
    """Check if the path points to stdout."""
    try:
        return path == STDOUT_FILEPATH or path.resolve() == STDOUT_FILEPATH
    except OSError:
        return False


def validate_b2_file_name_as_path(file_name: str) -> None:
    """
    Ensure a B2 file name is safe to interpret as a local path.
    """
    if RELATIVE_PATH_MATCHER.search(file_name):
        raise ValueError('File names containing relative path components are not supported')

    if ABSOLUTE_PATH_MATCHER.search(file_name):
        raise ValueError('File names containing absolute path components are not supported')

    if _IS_WINDOWS and DRIVE_MATCHER.search(file_name):
        raise ValueError('File names containing Windows drive letters are not supported')
