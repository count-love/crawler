"""
Copyright 2021, Count Love Crawler

Configures logging to output to a "crawl.log" file, this import that needs to be included first.
"""

import os
import logging

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s',
                    filename=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'crawl.log'), filemode='w',
                    level=logging.DEBUG)
