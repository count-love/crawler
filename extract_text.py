import bs4
import html
import math
import re
import urllib
import sys  # used for main

from utility import is_valid_absolute_url, unneeded_tags

"""
An HTML -> article text converter inspired by Readability's old open source library. Changes made to reflect our needs
(less videos, less galleries) and to support more HTML 5 tags.
"""
class ExtractText:
    aggressive = True

    unneeded_tags = unneeded_tags()

    cleanable_tags = ['form', 'h1']
    conditionally_cleanable_tags = ['table', 'ul', 'div', 'nav']

    score_tag = {'div': 5, 'article': 5, 'pre': 3, 'td': 3, 'blockquote': 3, 'main': 3, 'section': 1, 'span': -1,
                 'header': -3, 'footer': -3, 'address': -3,
                 'ol': -3, 'ul': -3, 'dl': -3, 'dd': -3, 'dt': -3, 'li': -3,
                 'form': -3,
                 'aside': -3, 'nav': -3, 'h1': -5, 'h2': -5, 'h3': -5, 'h4': -5, 'h5': -5, 'h6': -5, 'th': -5}

    re_display_none = re.compile(r'(^|;)\s*display\s*:\s*none\s*(;|$)', re.IGNORECASE)
    re_br_close = re.compile(r'<br[^>]*/>\s*', re.IGNORECASE)
    re_unlikely = re.compile(r'combx|comment|disqus|foot|header|menu|meta|nav|rss|shoutbox|sidebar|sponsor')
    re_hidden = re.compile(r'collapsible closed')
    re_maybe = re.compile(r'and|article|body|column|main|content|page')
    re_positive_indicators = re.compile(r'article|body|content|entry|hentry|page|pagination|post|text', re.IGNORECASE)
    re_negative_indicators = re.compile(r'combx|comment|contact|foot|footer|footnote|link|media|meta|promo|related|scroll|shoutbox|sponsor|tags|widget|gallery|abridged|closed|toggle', re.IGNORECASE)
    re_skip_div_to_p = re.compile(r'^(a|blockquote|dl|div|img|ol|p|pre|table|ul)', re.IGNORECASE)
    re_double_space = re.compile(r'[ \t]{2,}')
    re_punctuation = re.compile(r'[?.!]')

    def __init__(self):
        pass

    def _site_specific_preprocessing(self, soup, url):
        o = urllib.parse.urlparse(url)
        host = re.sub(r'(^www\.|\:\d+$)', '', o.netloc.lower())

        if host == 'thehill.com':
            # unneeded tags
            for el in soup.find_all('a', class_=re.compile(r'\b(people-articles|more)\b')):
                el.extract()

    def extract(self, html, url=None):
        # pre-process
        clean_html = self._process_html_string(html)

        # convert to soup
        soup = bs4.BeautifulSoup(clean_html, 'lxml')

        # site specific
        if url is not None:
            self._site_specific_preprocessing(soup, url)

        # other meta data
        meta_data = self._extract_meta_data(soup, url)

        # remove unneeded tags (script, style, iframe, link, embedded plugins)
        self._remove_unneeded_tags(soup)

        # convert double br to paragraph
        self._convert_double_br_to_p(soup)

        # remove unlikely tags (comments, etc)
        if self.aggressive:
            self._remove_unlikely_blocks(soup)

        # convert text div elements to p tags
        self._convert_div_to_p(soup)

        # get top score element
        content_el, content_score = self._get_top_score_tag(soup)

        # no content found?
        if content_el is None:
            # try less aggressive
            if self.aggressive:
                self.aggressive = False
                ret = self.extract(html, url)
                self.aggressive = True
                return ret

            # try cleaning body
            content_el = soup.find('body')
            if content_el is None:
                return None, None, meta_data
        elif content_score < 10. and self.aggressive:
            self.aggressive = False
            ret = self.extract(html, url)
            self.aggressive = True
            return ret
        else:
            # extend to siblings
            content_el = self._find_sibling_tags(soup, content_el)

        # perform clean up
        self._clean_article(content_el, url)

        # convert to text
        content_txt = self._content_to_text(content_el)

        return content_el, content_txt, meta_data

    def _extract_meta_data(self, soup, url):
        ret = {}

        tag_title = soup.find('title')
        if tag_title:
            ret['title'] = self._get_inner_text(tag_title)

        tag_canonical = soup.find('link', {'rel': 'canonical'})
        if tag_canonical:
            # unescape
            tag_canonical_url = html.unescape(tag_canonical.get('href'))

            if url:
                # turn into an absolute link
                tag_canonical_url = urllib.parse.urljoin(url, tag_canonical_url)
            elif '://' not in tag_canonical_url:
                # if not absolute, do not return it
                tag_canonical_url = None

            if tag_canonical_url and is_valid_absolute_url(tag_canonical_url):
                ret['canonical_url'] = tag_canonical_url

        return ret

    def _process_html_string(self, html):
        html = self.re_br_close.sub('<br>', html)

        return html

    def _convert_double_br_to_p(self, soup):
        found = True
        while found:
            found = False

            for el in soup.find_all('br'):
                ns = el.next_sibling
                if isinstance(ns, bs4.NavigableString):
                    if str(ns).strip():
                        continue
                    else:
                        ns = ns.next_sibling
                if ns is None:
                    continue
                if ns.name != 'br':
                    continue

                found = True

                if el.parent.name == 'p':
                    new_p = soup.new_tag('p')
                    for nns in list(ns.next_siblings):
                        new_p.append(nns.extract())
                    el.parent.insert_after(new_p)
                else:
                    # move previous siblings
                    new_p1 = soup.new_tag('p')
                    for ps in list(el.previous_siblings):
                        new_p1.insert(0, ps.extract())

                    # move next siblings
                    new_p2 = soup.new_tag('p')
                    for nns in list(ns.next_siblings):
                        new_p2.append(nns.extract())

                    el.parent.append(new_p1)
                    el.parent.append(new_p2)

                # remove <br>
                el.extract()
                ns.extract()

                break

    def _remove_unneeded_tags(self, soup):
        # unneeded tags
        for el in soup.find_all(self.unneeded_tags):
            if el.name == 'link':
                # parse sometimes view link as open / close tags, can remove important content
                el.unwrap()
            else:
                el.extract()

        # hidden tags
        for el in soup.find_all(style=self.re_display_none):
            el.extract()

    def _remove_unlikely_blocks(self, soup):
        for el in soup.find_all(True):
            # skip main tags
            if el.name == 'html' or el.name == 'body':
                continue

            # assemble string for matching
            s = el.get('id') or ""
            attr_class = el.get('class')
            if attr_class:
                if isinstance(attr_class, list):
                    s += " ".join(attr_class)
                else:
                    s += attr_class

            # remove element
            if self.re_unlikely.search(s) and not self.re_maybe.search(s):
                el.extract()

            # remove hidden elements
            if self.re_hidden.search(s):
                el.extract()

    def _convert_div_to_p(self, soup):
        for el in soup.find_all('div'):
            if el.find(self.re_skip_div_to_p) is None:
                el.name = 'p'
            else:
                # from readability's experimental code
                # wrap inside text in paragraph tags
                for child in el.contents:
                    if isinstance(child, bs4.element.NavigableString):
                        p = soup.new_tag('p')
                        p.attrs['style'] = 'display:inline;'
                        child.wrap(p)

    def _get_class_weight(self, el):
        score = 0.

        # score role
        attr_role = el.get('role')
        if attr_role:
            if attr_role == 'main' or attr_role == 'article':
                score += 25.
            if attr_role == 'navigation':
                score -= 25.

        # score class
        attr_class = el.get('class')
        if attr_class:
            if isinstance(attr_class, list):
                attr_class = " ".join(attr_class)
            if self.re_positive_indicators.search(attr_class):
                score += 25.
            if self.re_negative_indicators.search(attr_class):
                score -= 25.

        # id
        attr_id = el.get('id')
        if attr_id:
            if self.re_positive_indicators.search(attr_id):
                score += 25.
            if self.re_negative_indicators.search(attr_id):
                score -= 25.

        return score

    def _initial_score(self, el):
        score = 0.

        # score tag
        tag = el.name.lower()
        if tag in self.score_tag:
            score += self.score_tag[tag]

        # score class / ID
        score += self._get_class_weight(el)

        return score

    def _get_inner_text(self, el):
        # get text
        text = el.get_text()

        # remove spaces
        text = self.re_double_space.sub(' ', text.strip())

        # unescape
        text = html.unescape(text)

        return text

    def _add_score_to_tag(self, el, content_score):
        # get initial score
        if 'data-etscore' not in el.attrs:
            el['data-etscore'] = self._initial_score(el)

        el['data-etscore'] += content_score

    def _get_link_density(self, el):
        text_len = len(self._get_inner_text(el))
        link_len = 0

        # avoid division by zero
        if 0 == text_len:
            return 0.

        for a in el.find_all('a'):
            link_len += len(self._get_inner_text(a))

        return float(link_len) / float(text_len)

    def _get_top_score_tag(self, soup):
        # reset scores
        for el in soup.find_all(attrs={'data-etscore': True}):
            del el['data-etscore']

        # get paragraph tags
        for el in soup.find_all('p'):
            # get text
            text = self._get_inner_text(el)

            # too little text
            if len(text) < 25 or not self.re_punctuation.search(text):
                continue

            # content scoring
            content_score = 1.

            # add points for commas
            content_score += float(len(text.split(',')))

            # every one hundred characters adds 1 point, up to 3 points
            content_score += min(math.floor(len(text) / 100), 3.)

            # add to parents
            for parent in el.parents:
                if not parent:
                    break

                # add score
                self._add_score_to_tag(parent, content_score)

                # divide content score
                content_score /= 2.

                if content_score < 1:
                    break

        # scale by link density
        top_candidate = None
        top_candidate_score = None
        for el in soup.find_all(attrs={'data-etscore': True}):
            # get link density
            link_density = self._get_link_density(el)

            # adjust score
            el['data-etscore'] *= 1. - link_density

            # is top candidate
            if top_candidate is None or el['data-etscore'] > top_candidate_score:
                top_candidate = el
                top_candidate_score = el['data-etscore']

        return top_candidate, top_candidate_score

    def _find_sibling_tags(self, soup, top_candidate):
        if top_candidate.parent is None:
            return top_candidate

        # make list of elements
        elements = []
        threshold = max(10., 0.2 * top_candidate['data-etscore'])
        for el in top_candidate.parent.contents:
            # keep top candidate
            if el == top_candidate:
                elements.append(el)
                continue

            # is string
            if isinstance(el, bs4.element.NavigableString):
                # check text
                text_len = len(el.string.strip())

                # skip it?
                if text_len < 80 and re.search(r'\.(\s|$)', el.string) is None:
                    continue

                # append it
                elements.append(el)
                continue

            # check score
            if 'data-etscore' not in el.attrs or threshold > el['data-etscore']:
                continue

            if el.name == "p":
                text = self._get_inner_text(el)
                text_len = len(text)
                link_density = self._get_link_density(el)

                if link_density >= 0.25:
                    continue
                if text_len < 80 and (link_density > 0 or re.search(r'\.(\s|$)', text) is None):
                    continue

            # append it
            elements.append(el)

        # has siblings
        if len(elements) > 1:
            new_el = soup.new_tag('div')
            for el in elements:
                new_el.append(el)
            return new_el

        return top_candidate

    def _should_clean_conditional(self, el):
        weight = self._get_class_weight(el)

        if weight < 0:
            return True

        # get text
        text = self._get_inner_text(el)

        # comma count
        commas = len(text.split(','))
        if commas > 10:
            return False

        # compare other counts
        count_p = len(el.find_all('p'))
        count_img = len(el.find_all('img'))
        count_li = len(el.find_all('li'))
        count_input = len(el.find_all('input'))
        link_density = self._get_link_density(el)
        text_len = len(text)

        if count_img > count_p:
            return True
        if count_li > count_p and el.name != 'ul' and el.name != 'ol':
            return True
        if count_input > count_p / 3:
            return True
        if text_len < 25 and (count_img == 0 or count_img > 2):
            return True
        if weight < 25 and link_density > 0.2:
            return True
        if link_density > 0.5:
            return True

        return False

    def _clean_article(self, content_el, url=None):
        # remove elements
        for tag_name in self.cleanable_tags:
            for el in content_el.find_all(tag_name):
                el.extract()

        # conditionally clean
        for tag_name in self.conditionally_cleanable_tags:
            for el in content_el.find_all(tag_name):
                if self._should_clean_conditional(el):
                    el.extract()

        # make links and images absolute
        if url is not None:
            for el in content_el.find_all(href=True):
                try:
                    el['href'] = urllib.parse.urljoin(url, el['href'])
                except:
                    pass
            for el in content_el.find_all(src=True):
                try:
                    el['src'] = urllib.parse.urljoin(url, el['src'])
                except:
                    pass

        # remove empty paragraphs
        for el in content_el.find_all('p'):
            img_count = el.find_all('img')
            if len(img_count) == 0 and self._get_inner_text(el) == '':
                el.extract()

        # remove
        return content_el

    def _content_to_text(self, el):
        # convert to text
        text = self._element_to_text(el)

        # strip double spaces
        text = self.re_double_space.sub(' ', text)

        # strip more than two line breaks
        text = re.sub(r'(\n[ \t]*){2,}\n', '\n\n', text)

        return text.strip()

    def _element_to_text(self, el):
        # is comment?
        # could potentially suppress bs4.element.PreformattedString
        if isinstance(el, bs4.element.Comment):
            return ''

        # string?
        if isinstance(el, bs4.element.NavigableString):
            # convert to text
            text = el.string

            # unescape
            return html.unescape(text)

        # make name lower case
        tag = el.name.lower()
        contents = ''.join([self._element_to_text(x) for x in el.contents])

        # paragraphs
        if tag == 'p':
            return contents.strip() + '\n\n'

        # line breaks
        if tag == 'br':
            return '\n'

        # horizontal rules
        if tag == 'hr':
            return ('-' * 18) + '\n\n'

        # list items
        if tag == 'li':
            return '* ' + contents.strip() + '\n'

        # list containers
        if tag == 'ol' or tag == 'ul':
            return contents.strip() + '\n\n'

        # headings
        m = re.match(r'h(\d+)', tag)
        if m:
            return ('#' * int(m.group(1))) + ' ' + contents.strip() + '\n\n'

        # italic
        if (tag == 'i' or tag == 'em') and contents.strip():
            return ' *' + contents + '* '

        # bold
        if (tag == 'b' or tag == 'strong') and contents.strip():
            return ' **' + contents + '** '

        # links (maybe?)
        if tag == 'a' and 'href' in el.attrs and el['href'][0:4] == 'http':
            # do not print link if no content
            if contents == '':
                return ''

            return '[' + contents + '](' + el['href'] + ')'

        return contents


def mode_apply():
    # read all of standard in
    html = ''.join([l for l in sys.stdin.readlines()])

    # make extractor
    ex = ExtractText()

    # extract
    _, txt, meta = ex.extract(html)

    # print
    print(meta)
    print(txt)


# useful for debugging
def main():
    modes = ['help', 'apply', 'test']

    # determine mode
    if len(sys.argv) == 1:
        mode = 'apply'
    elif len(sys.argv) == 2:
        mode = sys.argv[1]
        if mode not in ['help', 'apply', 'test']:
            mode = 'error'
    elif len(sys.argv) == 3:
        mode = sys.argv[1]
        if mode not in ['debug']:
            mode = 'error'
    else:
        mode = 'error'

    # print help
    if mode == 'error' or mode == 'help':
        print('Usage:')
        print('')
        print('\t%s [mode]' % sys.argv[0])
        print('')
        print('Parameter mode can be one of: %s.' % ', '.join([x for x in modes if x != 'error']))
        print('')
        sys.exit(0 if mode == 'help' else 1)

    if mode == 'apply':
        mode_apply()


if __name__ == "__main__":
    main()
