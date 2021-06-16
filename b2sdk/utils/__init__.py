######################################################################
#
# File: b2sdk/utils/__init__.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import base64
import hashlib
import os
import platform
import re
import shutil
import tempfile
import time
import concurrent.futures as futures
from decimal import Decimal
from urllib.parse import quote, unquote_plus

from logfury.v0_1 import DefaultTraceAbstractMeta, DefaultTraceMeta, limit_trace_arguments, disable_trace, trace_call


def interruptible_get_result(future):
    """
    Wait for the result of a future in a way that can be interrupted
    by a KeyboardInterrupt.

    This is not necessary in Python 3, but is needed for Python 2.

    :param future: a future to get result of
    :type future: Future
    """
    while True:
        try:
            return future.result(timeout=1.0)
        except futures.TimeoutError:
            pass


def b2_url_encode(s):
    """
    URL-encode a unicode string to be sent to B2 in an HTTP header.

    :param s: a unicode string to encode
    :type s: str
    :return: URL-encoded string
    :rtype: str
    """
    return quote(s.encode('utf-8'))


def b2_url_decode(s):
    """
    Decode a Unicode string returned from B2 in an HTTP header.

    :param s: a unicode string to decode
    :type s: str
    :return: a Python unicode string.
    :rtype: str
    """
    return unquote_plus(s)


def choose_part_ranges(content_length, minimum_part_size):
    """
    Return a list of (offset, length) for the parts of a large file.

    :param content_length: content length value
    :type content_length: int
    :param minimum_part_size: a minimum file part size
    :type minimum_part_size: int
    :rtype: list
    """

    # If the file is at least twice the minimum part size, we are guaranteed
    # to be able to break it into multiple parts that are all at least
    # the minimum part size.
    assert minimum_part_size * 2 <= content_length

    # How many parts can we make?
    part_count = min(content_length // minimum_part_size, 10000)
    assert 2 <= part_count

    # All of the parts, except the last, are the same size.  The
    # last one may be bigger.
    part_size = content_length // part_count
    last_part_size = content_length - (part_size * (part_count - 1))
    assert minimum_part_size <= last_part_size

    # Make all of the parts except the last
    parts = [(i * part_size, part_size) for i in range(part_count - 1)]

    # Add the last part
    start_of_last = (part_count - 1) * part_size
    last_part = (start_of_last, content_length - start_of_last)
    parts.append(last_part)

    return parts


def hex_sha1_of_stream(input_stream, content_length):
    """
    Return the 40-character hex SHA1 checksum of the first content_length
    bytes in the input stream.

    :param input_stream: stream object, which exposes read() method
    :param content_length: expected length of the stream
    :type content_length: int
    :rtype: str
    """
    remaining = content_length
    block_size = 1024 * 1024
    digest = hashlib.sha1()
    while remaining != 0:
        to_read = min(remaining, block_size)
        data = input_stream.read(to_read)
        if len(data) != to_read:
            raise ValueError(
                'content_length(%s) is more than the size of the file' % content_length
            )
        digest.update(data)
        remaining -= to_read
    return digest.hexdigest()


def hex_sha1_of_unlimited_stream(input_stream, limit=None):
    block_size = 1024 * 1024
    content_length = 0
    digest = hashlib.sha1()
    while True:
        if limit is not None:
            to_read = min(limit - content_length, block_size)
        else:
            to_read = block_size
        data = input_stream.read(to_read)
        data_len = len(data)
        if data_len > 0:
            digest.update(data)
            content_length += data_len
        if data_len < to_read:
            return digest.hexdigest(), content_length


def hex_sha1_of_file(path_):
    with open(
        str(path_), 'rb'
    ) as file:  # TODO: remove str() after dropping python 3.5 support, it's here in
        # case someone uses pathlib.Paths
        return hex_sha1_of_unlimited_stream(file)


def hex_sha1_of_bytes(data: bytes) -> str:
    """
    Return the 40-character hex SHA1 checksum of the data.
    """
    return hashlib.sha1(data).hexdigest()


def hex_md5_of_bytes(data: bytes) -> str:
    """
    Return the 32-character hex MD5 checksum of the data.
    """
    return hashlib.md5(data).hexdigest()


def md5_of_bytes(data: bytes) -> bytes:
    """
    Return the 16-byte MD5 checksum of the data.
    """
    return hashlib.md5(data).digest()


def b64_of_bytes(data: bytes) -> str:
    """
    Return the base64 encoded represtantion of the data.
    """
    return base64.b64encode(data).decode()


def validate_b2_file_name(name):
    """
    Raise a ValueError if the name is not a valid B2 file name.

    :param name: a string to check
    :type name: str
    """
    if not isinstance(name, str):
        raise ValueError('file name must be a string, not bytes')
    name_utf8 = name.encode('utf-8')
    if len(name_utf8) < 1:
        raise ValueError('file name too short (0 utf-8 bytes)')
    if 1000 < len(name_utf8):
        raise ValueError('file name too long (more than 1000 utf-8 bytes)')
    if name[0] == '/':
        raise ValueError("file names must not start with '/'")
    if name[-1] == '/':
        raise ValueError("file names must not end with '/'")
    if '\\' in name:
        raise ValueError("file names must not contain '\\'")
    if '//' in name:
        raise ValueError("file names must not contain '//'")
    if chr(127) in name:
        raise ValueError("file names must not contain DEL")
    if any(250 < len(segment) for segment in name_utf8.split(b'/')):
        raise ValueError("file names segments (between '/') can be at most 250 utf-8 bytes")


def is_file_readable(local_path, reporter=None):
    """
    Check if the local file has read permissions.

    :param local_path: a file path
    :type local_path: str
    :param reporter: reporter object to put errors on
    :rtype: bool
    """
    if not os.path.exists(local_path):
        if reporter is not None:
            reporter.local_access_error(local_path)
        return False
    elif not os.access(local_path, os.R_OK):
        if reporter is not None:
            reporter.local_permission_error(local_path)
        return False
    return True


def get_file_mtime(local_path):
    """
    Get modification time of a file in milliseconds.

    :param local_path: a file path
    :type local_path: str
    :rtype: int
    """
    mod_time = os.path.getmtime(local_path) * 1000
    return int(mod_time)


def set_file_mtime(local_path, mod_time_millis):
    """
    Set modification time of a file in milliseconds.

    :param local_path: a file path
    :type local_path: str
    :param mod_time_millis: time to be set
    :type mod_time_millis: int
    """
    mod_time = mod_time_millis / 1000.0

    # We have to convert it this way to avoid differences when mtime
    # is read from the local file in the next iterations, and time is fetched
    # without rounding.
    # This is caused by floating point arithmetic as POSIX systems
    # represents mtime as floats and B2 as integers.
    # E.g. for 1093258377393, it would be converted to 1093258377.393
    # which is actually represented by 1093258377.3929998874664306640625.
    # When we save mtime and read it again, we will end up with 1093258377392.
    # See #617 for details.
    mod_time = float(Decimal('%.3f5' % mod_time))

    os.utime(local_path, (mod_time, mod_time))


def fix_windows_path_limit(path):
    """
    Prefix paths when running on Windows to overcome 260 character path length limit.
    See https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx#maxpath

    :param path: a path to prefix
    :type path: str
    :return: a prefixed path
    :rtype: str
    """
    if platform.system() == 'Windows':
        if path.startswith('\\\\'):
            # UNC network path
            return '\\\\?\\UNC\\' + path[2:]
        elif os.path.isabs(path):
            # local absolute path
            return '\\\\?\\' + path
        else:
            # relative path, don't alter
            return path
    else:
        return path


class TempDir(object):
    """
    Context manager that creates and destroys a temporary directory.
    """

    def __enter__(self):
        """
        Return the unicode path to the temp dir.
        """
        dirpath_bytes = tempfile.mkdtemp()
        self.dirpath = str(dirpath_bytes.replace('\\', '\\\\'))
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.dirpath)
        return None  # do not hide exception


def _pick_scale_and_suffix(x):
    # suffixes for different scales
    suffixes = ' kMGTP'

    # We want to use the biggest suffix that makes sense.
    ref_digits = str(int(x))
    index = (len(ref_digits) - 1) // 3
    suffix = suffixes[index]
    if suffix == ' ':
        suffix = ''

    scale = 1000**index
    return (scale, suffix)


def format_and_scale_number(x, unit):
    """
    Pick a good scale for representing a number and format it.

    :param x: a number
    :type x: int
    :param unit: an arbitrary unit name
    :type unit: str
    :return: scaled and formatted number
    :rtype: str
    """

    # simple case for small numbers
    if x < 1000:
        return '%d %s' % (x, unit)

    # pick a scale
    (scale, suffix) = _pick_scale_and_suffix(x)

    # decide how many digits after the decimal to display
    scaled = x / scale
    if scaled < 10.0:
        fmt = '%1.2f %s%s'
    elif scaled < 100.0:
        fmt = '%1.1f %s%s'
    else:
        fmt = '%1.0f %s%s'

    # format it
    return fmt % (scaled, suffix, unit)


def format_and_scale_fraction(numerator, denominator, unit):
    """
    Pick a good scale for representing a fraction, and format it.

    :param numerator: a numerator of a fraction
    :type numerator: int
    :param denominator: a denominator of a fraction
    :type denominator: int
    :param unit: an arbitrary unit name
    :type unit: str
    :return: scaled and formatted fraction
    :rtype: str
    """

    # simple case for small numbers
    if denominator < 1000:
        return '%d / %d %s' % (numerator, denominator, unit)

    # pick a scale
    (scale, suffix) = _pick_scale_and_suffix(denominator)

    # decide how many digits after the decimal to display
    scaled_denominator = denominator / scale
    if scaled_denominator < 10.0:
        fmt = '%1.2f / %1.2f %s%s'
    elif scaled_denominator < 100.0:
        fmt = '%1.1f / %1.1f %s%s'
    else:
        fmt = '%1.0f / %1.0f %s%s'

    # format it
    scaled_numerator = numerator / scale
    return fmt % (scaled_numerator, scaled_denominator, suffix, unit)


_CAMELCASE_TO_UNDERSCORE_RE = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')


def camelcase_to_underscore(input_):
    """
    Convert a camel-cased string to a string with underscores.

    :param input_: an input string
    :type input_: str
    :return: string with underscores
    :rtype: str
    """
    return _CAMELCASE_TO_UNDERSCORE_RE.sub(r'_\1', input_).lower()


class B2TraceMeta(DefaultTraceMeta):
    """
    Trace all public method calls, except for ones with names that begin with `get_`.
    """
    pass


class B2TraceMetaAbstract(DefaultTraceAbstractMeta):
    """
    Default class for tracers, to be set as
    a metaclass for abstract base classes.
    """
    pass


class ConcurrentUsedAuthTokenGuard(object):
    """
    Context manager preventing two tokens being used simultaneously.
    Throws UploadTokenUsedConcurrently when unable to acquire a lock
    Sample usage:

    with ConcurrentUsedAuthTokenGuard(lock_for_token, token):
        # code that uses the token exclusively
    """

    def __init__(self, lock, token):
        self.lock = lock
        self.token = token

    def __enter__(self):
        if not self.lock.acquire(False):
            from b2sdk.exception import UploadTokenUsedConcurrently
            raise UploadTokenUsedConcurrently(self.token)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.lock.release()
        except RuntimeError:
            # guard against releasing a non-acquired lock
            pass


def current_time_millis():
    """
    File times are in integer milliseconds, to avoid roundoff errors.
    """
    return int(round(time.time() * 1000))


assert disable_trace
assert limit_trace_arguments
assert trace_call
