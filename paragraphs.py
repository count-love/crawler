"""
Copyright 2020, Count Love Crawler

Set of regular expressions for stripping formatting characters and
extracting paragraph like text.
"""

import re


re_line = re.compile(r'\n+')
re_is_paragraph = re.compile(r'[A-Z].*[a-z].+[\.!\?]')
re_strip_headings = re.compile(r'^#+ ') # strip markdown headings
re_strip_format = re.compile(r' \*{1,2}([^\*]+)\*{1,2} ') # strip markdown bold and italic
re_strip_links = re.compile(r'\[([^\]]+)\]\(https?://[^)]+\)', flags=re.IGNORECASE) # strip markdown links


def is_paragraph(para):
    # minimum length
    if len(para) < 20:
        return False

    # test using regular expression
    if not re_is_paragraph.search(para):
        return False

    return True


def generate_paragraphs_from_text(text):
    # clean markdown
    text = re_strip_headings.sub('', text)
    text = re_strip_format.sub(r' \1 ', text)
    text = re_strip_links.sub(r'\1', text)

    # split into lines
    paragraphs = re_line.split(text)

    # find likely paragraphs
    for para in paragraphs:
        if is_paragraph(para):
            yield para
