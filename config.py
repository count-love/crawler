"""
Copyright 2021, Count Love Crawler

Configuration file with constants used. Currently only contains database
configuration information.
"""

import os


class Config:
    def __init__(self):
        self.sqlite_db = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data.db')

        self.mailgun_domain = ""
        self.mailgun_api_key = ""

        self.email_from = ""
        self.email_queue = []
