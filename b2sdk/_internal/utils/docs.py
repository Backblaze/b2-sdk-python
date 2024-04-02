######################################################################
#
# File: b2sdk/_internal/utils/docs.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations


class MissingDocURL(Exception):
    pass


def ensure_b2sdk_doc_urls(cls: type):
    """
    Decorator to indicate (and verify) that class has external documentation URLs.

    Used for to validate that all classes have external documentation URLs properly defined.
    """
    urls = get_b2sdk_doc_urls(cls)
    if not urls:
        raise MissingDocURL(f'No documentation URLs found for {cls.__name__}')
    return cls


def get_b2sdk_doc_urls(type_: type) -> dict[str, str]:
    """
    Get the external documentation URLs for a b2sdk class.

    Non-b2sdk classes are not, and will not be supported.

    :param type_: the class to get the documentation URLs for
    :return: a dictionary mapping link names to URLs
    """
    docstring = type_.__doc__
    if not docstring:
        return {}
    return _extract_restructedtext_links(docstring)


_rest_link_prefix = '.. _'


def _extract_restructedtext_links(docstring: str) -> dict[str, str]:
    links = {}
    for line in docstring.splitlines():
        line = line.strip()
        if line.startswith(_rest_link_prefix):
            name, url = line[len(_rest_link_prefix):].split(': ', 1)
            if name and url:
                links[name] = url
    return links
