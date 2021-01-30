"""
Copyright 2021, Count Love Crawler

Email recent crawl results. Requires Mailgun configuration information in the
config.py file. Will email results crawled in the last 16 hours.

> python email_crawl.py
"""

import re
import logging
import requests  # pip install requests

import common
from config import Config


def send_email(recipients, subject, body_text, body_html=None, from_name=None, from_email=None):
    c = Config()

    # url
    url = "https://api.mailgun.net/v3/%s/messages" % c.mailgun_domain

    # email from
    if from_email is None:
        em_from = "\"%s\" <%s@%s>" % ((from_name or "No Reply"), "no-reply", c.mailgun_domain)
    elif from_name is None:
        em_from = from_email
    else:
        em_from = "\"%s\" <%s>" % (from_name, from_email)

    # email to
    if isinstance(recipients, list):
        em_to = recipients
    elif isinstance(recipients, tuple):
        em_to = list(recipients)
    else:
        em_to = [recipients]

    # build request
    req_data = {
        "from": em_from,
        "to": em_to,
        "subject": subject,
        "text": body_text,
        "o:tracking-clicks": False
    }

    # has html?
    if body_html is not None:
        req_data["html"] = body_html

    # run response
    response = requests.post(url,
                             auth=("api", c.mailgun_api_key),
                             data=req_data)

    # check response
    response.raise_for_status()


def get_queue_urls(db_cursor):
    # find sources
    db_cursor.execute('SELECT CrawlerQueue.ID, CrawlerQueue.Name, CrawlerQueue.Location, CrawlerQueue.Source, CrawlerQueue.AddedOn '
                      'FROM CrawlerQueue '
                      'WHERE CrawlerQueue.AddedOn > DATETIME(\'now\', \'-16 hours\') '
                      'ORDER BY CrawlerQueue.Category ASC')

    # get sources
    return [x for x in db_cursor.fetchall()]


def send_queue_urls(db_cursor):
    c = Config()

    # already emailed list (only check once)
    already_emailed = set()

    email_addresses = c.email_queue

    # get urls
    queue_rows = get_queue_urls(db_cursor)

    # no URLs
    if len(queue_rows) == 0:
        return

    # subject
    most_recent = max([x['AddedOn'] for x in queue_rows])
    subject = "Count Love crawl - potential articles from %s" % most_recent # .strftime("%A, %B %-d, %Y")

    # body
    body = "Below are a list of the links and the associated text that we " \
           "found on local news sites from around the country:\n\n"

    # add the crawl URLs
    re_nl = re.compile(r'[\r\n]+')
    body += "\n\n".join(["%s\n%s" % (re_nl.sub(' ', x['Name']), x['Source']) for x in queue_rows])

    # no one to email
    if len(email_addresses) == 0:
        print(body)
        return

    # no email key
    if not c.mailgun_domain or not c.mailgun_api_key:
        raise Exception('No email domain or API key specified.')

    # for each recipient
    for recipient in email_addresses:
        if recipient not in already_emailed:
            logging.warning('Skipping email: %s (emailed since most recent event).', recipient)
            continue

        # add to already emailed list
        already_emailed.add(recipient)

        # send email
        logging.info('Emailing crawl to %s', recipient)
        send_email(recipients=recipient, subject=subject, body_text=body,
                   from_name="Count Love", from_email=c.email_from)


def main():
    # connect to database
    conn = common.get_db_connection()
    c = conn.cursor()

    # fetch sources
    send_queue_urls(c)

    # close connection
    c.close()
    conn.close()


if __name__ == "__main__":
    main()
