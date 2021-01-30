"""
Copyright 2021, Count Love Crawler

Main crawler logic used for crawling page, extracting text,
handling encoding conversion, and finalizing the crawl results.

Steps:
1. Crawl enabled sources in the Sources table looking for links
   matching regular expression.
2. Crawl articles found in step 1, extracting text.
3. Sort articles by text similarity.
4. Finalize crawl results (email or export).

Crawling can be invoked by calling:

> python crawler.py
"""

# noinspection PyUnresolvedReferences
import log_file  # enable file logging by default (must be first import)
import abc
from bs4 import BeautifulSoup
from datetime import datetime
import html
import re
import urllib
import logging
import requests
from ftfy import fix_encoding
import sqlite3 as db

import common
from extract_text import ExtractText
from categorize import sort_articles_by_similarity
from email_crawl import send_queue_urls
from utility import md5

# static variables
space_regex = re.compile(r'\s+')
content_type_regex = re.compile(r'\btext/.+', re.IGNORECASE)
anchor_regex = re.compile('#.+')
clean_url_regex = re.compile('&TM=[.0-9]+$')  # strip unneeded parameters from the URL


class CrawlerConfiguration(object):
    seen_titles = set()

    def __init__(self):
        pass

    """
    Regular expression, required. Keywords to look for when crawling main pages.
    """
    words_regex = None

    """
    Should return either a regular expression or none. If a regular expression is returned, any links where the link
    text matches the regular expression are excluded.
    """
    exclude_title_regex = None

    """
    Should return either a regular expression or none. If a regular expression is returned, any links where the link 
    URL matches the regular expression are excluded.
    """
    exclude_url_regex = None

    def start_crawl(self):
        self.seen_titles = set()

    def end_crawl(self):
        pass

    @abc.abstractmethod
    def add_result(self, source_id, title, link, location):
        pass

    # used by crawler
    def _has_seen_title(self, title):
        if title in self.seen_titles:
            return True

        # add it
        self.seen_titles.add(title)

        return False

    def _skip_title(self, title):
        return self.exclude_title_regex is not None and self.exclude_title_regex.search(title)

    def _skip_url(self, link):
        return self.exclude_url_regex is not None and self.exclude_url_regex.search(link)


class CountLoveConfiguration(CrawlerConfiguration):
    date_string = None

    words_regex = re.compile(r'\b(protest(|ed|ers)|march(|ed)|demonstr(ated|ation|ators)|rall(y|ies|ied))\b')
    exclude_title_regex = re.compile(r'\b((basket|base|foot|soft|volley)ball|overtime|second\shalf|((first|third|fourth)\squarter)|gallery|letter|photos?|videos?|slideshow|pep\srall(y|ies)|stock\smarket)\b', re.IGNORECASE)
    exclude_url_regex = re.compile(r'\b(videos?|inphotos|photos?|sports?|opinion|editorial|picture-gallery|local_events|clip|columnists|galleries|photogallery|image)\b', re.IGNORECASE)

    def __init__(self, db_conn, db_cursor):
        super(CountLoveConfiguration, self).__init__()

        # store database
        self.db_conn = db_conn
        self.db_cursor = db_cursor

    def start_crawl(self):
        super(CountLoveConfiguration, self).start_crawl()

        # store date string
        self.date_string = datetime.today().strftime('%Y-%m-%d')

    def end_crawl(self):
        super(CountLoveConfiguration, self).end_crawl()

        self.db_conn.commit()

    def add_result(self, source_id, title, link, location):
        # bulk update
        self.db_cursor.execute('INSERT OR IGNORE INTO `CrawlerQueue` '
                               '(`Date`, `Name`, `Location`, `SourceID`, `Source`, `SourceHash`) VALUES '
                               '(?, ?, ?, ?, ?, ?)',
                               (self.date_string, title, location, source_id, link, md5(link)))


class Crawler(object):
    configs = []

    def __init__(self, db_conn, db_cursor):
        self.db_conn = db_conn
        self.db_cursor = db_cursor

    def add_config(self, config):
        self.configs.append(config)

    def run(self):
        # signal start of crawl
        for config in self.configs:
            config.start_crawl()

        # list of sources to crawl
        to_crawl = self._urls_to_crawl()

        # successfully cralwed
        successfully_crawled = []

        maxi = len(to_crawl)
        for i, (id, url, location) in enumerate(to_crawl):
            logging.debug('%05d/%05d: %s (%s)' % (i, maxi, url[0:24], location))

            # crawl it
            url, response = fetch_url(url)

            # process response
            if response is not None:
                success = self._process_response(id, url, location, response)
            else:
                success = False

            # if success?
            if success:
                successfully_crawled.append((datetime.now(), id))

        # signal end of crawl
        for config in self.configs:
            config.end_crawl()

        # mark as crawled
        if successfully_crawled:
            self.db_cursor.executemany('UPDATE Sources SET LastCrawled = ? WHERE ID = ?', successfully_crawled)
            self.db_conn.commit()

    def _urls_to_crawl(self):
        # find sources
        self.db_cursor.execute('SELECT ID, Source, Location FROM Sources WHERE Enabled = 1')

        # get sources
        ret = []
        for x in self.db_cursor.fetchall():
            ret.append((x['ID'], x['Source'], x['Location']))

        return ret

    def _clean_url(self, url):
        return clean_url_regex.sub('', url)

    def _process_response(self, id, url, location, response):
        # parse HTML into a BeautifulSoup document
        try:
            soup = BeautifulSoup(response, 'lxml')
        except Exception as e:
            logging.error('Parsing error for %s: %s', url, e)
            return False

        # for each configuration
        for config in self.configs:
            unique_urls = set()

            # extract links that contain words of interest
            for link in soup.find_all('a', attrs={'href': True}):
                # get link text
                link_text = html.unescape(link.get_text()).strip()

                # clean link text
                link_text = space_regex.sub(' ', link_text)

                # contains word?
                if not config.words_regex.search(link_text):
                    continue

                # already seen?
                if config._has_seen_title(link_text) or config._skip_title(link_text):
                    continue

                # link href
                link_href = link['href'].strip()

                # turn into an absolute link
                link_href = urllib.parse.urljoin(url, link_href)

                # remove anchor
                link_href = anchor_regex.sub("", link_href)

                # exclude by URL?
                if config._skip_url(link_href):
                    continue

                # ensure that it is an HTTP link
                link_parts = urllib.parse.urlparse(link_href)
                if link_parts.scheme.lower() != "http" and link_parts.scheme.lower() != "https":
                    continue

                # strip some URL parameters
                link_href = self._clean_url(link_href)

                # add to entry queue
                if link_href not in unique_urls:
                    config.add_result(id, link_text, link_href, location)
                    unique_urls.add(link_href)

        self.db_conn.commit()

        return True


def fetch_url(url):
    # create request object
    try:
        # fetch response
        response = requests.get(url, timeout=15., headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:85.0) Gecko/20100101 Firefox/85.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        })

        # raise exception on failure
        response.raise_for_status()
    except Exception as e:
        logging.error('Fetch failed %s: %s', url, e)
        return None, None

    # detect redirects
    if response.url != url and response.headers:
        logging.warning('Redirect %s to %s', url, response.url)

    return response.url, response.text


def crawl_sources(db_conn, db_cursor):
    # make crawler
    crawler = Crawler(db_conn, db_cursor)

    # configuration to add to internal processing queue
    crawler.add_config(CountLoveConfiguration(db_conn, db_cursor))

    # run
    crawler.run()


def articles_to_crawl(db_cursor):
    # find sources
    db_cursor.execute('SELECT ID, Source FROM CrawlerQueue WHERE '
                      'NOT EXISTS(SELECT * FROM CrawlerContent WHERE CrawlerContent.QueueID = CrawlerQueue.ID)')

    # to return
    return [(int(x['ID']), x['Source']) for x in db_cursor.fetchall()]


def fetch_content(request_url, extractor):
    meta = {}

    # fetch using python
    url, response = fetch_url(request_url)

    if response is not None:
        # extract content
        el, txt, meta = extractor.extract(response, url)

        if txt is not None and txt != '':
            txt = fix_encoding(txt)
            return url, response, txt, meta

    # give up
    return url if url else request_url, None, None, meta


def crawl_articles(db_conn, db_cursor):
    # get list of articles
    articles = articles_to_crawl(db_cursor)

    # extractor
    extractor = ExtractText()

    # track ids
    successful_ids = []

    for (id, url) in articles:
        logging.debug('Fetch %d: %s', id, url)

        # crawl it
        actual_url, html, txt, meta = fetch_content(url, extractor)

        # figure out canonical url, in case of redirect or meta tags
        if 'canonical_url' in meta and meta['canonical_url'] != actual_url:
            canonical_url = meta['canonical_url']
        elif actual_url:
            canonical_url = url
        else:
            canonical_url = url

        # update the URL if there is a canonical URL
        if canonical_url != url:
            try:
                db_cursor.execute('UPDATE CrawlerQueue SET Source = ?, SourceHash = ? WHERE CrawlerQueue.ID = ?',
                                  (canonical_url, md5(canonical_url), id))
            except db.IntegrityError:
                # URL is already crawled, remove new CrawlerQueueEntry and continue
                db_cursor.execute('DELETE FROM CrawlerQueue WHERE ID = ?', (id,))
                continue

        # has response
        placeholder = True
        if txt is not None:
            try:
                # insert crawler content
                db_cursor.execute('REPLACE INTO CrawlerContent (QueueID, Text) VALUES (?, ?)', (id, txt))

                # insert
                db_conn.commit()
                placeholder = False

                # append
                successful_ids.append(id)
            except db.InternalError:
                logging.error('DB encoding error: %d %s', id, url)
            except db.DataError:
                logging.error('DB data error (too much text): %d %s', id, url)

        if placeholder:
            # placeholder row (so it won't crawl again)
            db_cursor.execute('INSERT OR IGNORE INTO CrawlerContent (QueueID, Text) VALUES (?, ?)', (id, None))
            db_conn.commit()

    return successful_ids


def main():
    # connect to database
    db_connection = common.get_db_connection()
    db_cursor = db_connection.cursor()

    # fetch sources
    logging.info('(1/4) Crawl sources')
    crawl_sources(db_connection, db_cursor)

    # fetch articles
    logging.info('(2/4) Crawl articles')
    crawl_articles(db_connection, db_cursor)

    # categorize
    logging.info('(3/4) Group crawled articles by similarity')
    sort_articles_by_similarity(db_connection, db_cursor)

    # send crawl emails
    logging.info('(4/4) Send crawl email')
    send_queue_urls(db_cursor)

    # close connection
    db_cursor.close()
    db_connection.close()


if __name__ == "__main__":
    main()
