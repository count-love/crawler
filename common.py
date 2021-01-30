"""
Copyright 2021, Count Love Crawler

Code for instantiating database connection.
"""

import sqlite3 as db

from config import Config


def get_db_connection():
    c = Config()
    conn = db.connect(c.sqlite_db)
    conn.row_factory = db.Row
    return conn
