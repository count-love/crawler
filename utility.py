"""
Copyright 2021, Count Love Crawler

Common utility functions.
"""

import hashlib
from urllib.parse import urlparse


def is_valid_absolute_url(url):
    """
    Confirms that a URL is a valid http or https absolute address.
    :param url:
    :return:
    """
    parsed = urlparse(url)
    if parsed.scheme.lower() not in ['http', 'https']:
        return False
    if not parsed.netloc:
        return False
    return True


def unneeded_tags():
    return ['head', 'title', 'meta', 'script', 'noscript', 'style', 'iframe', 'embed', 'object', 'link']


def md5(str):
    return hashlib.md5(str.encode('utf8')).digest()
