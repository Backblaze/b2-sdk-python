######################################################################
#
# File: b2sdk/_internal/utils/http_date.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import datetime as dt


def parse_http_date(timestamp_str: str) -> dt.datetime:
    # See https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.1.1
    # for the list of supported formats.
    # We don't parse non-GTM dates because they are not valid HTTP-dates
    # as defined in RFC7231 7.1.1.1. Backblaze is more premissive than
    # the standard here.
    http_data_formats = [
        '%a, %d %b %Y %H:%M:%S GMT',  # IMF-fixdate
        '%A, %d-%b-%y %H:%M:%S GMT',  # obsolete RFC 850 format
        '%a %b %d %H:%M:%S %Y',  # ANSI C's asctime() format
    ]
    for format in http_data_formats:
        try:
            timestamp = dt.datetime.strptime(timestamp_str, format)
            return timestamp.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            pass
    raise ValueError("Value %s is not a valid HTTP-date, won't be parsed.", timestamp_str)
