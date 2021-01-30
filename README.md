# Count Love Crawler

## Installation

To isolate the crawler and its dependencies, it is recommended that you install
in a Python virtual environment. 

Tested with Python 3.9 (but should be compatible with a range of versions).

### Install dependencies

To install dependencies, run:

```shell
pip install -r requirements.txt
```

### Setup SQLite database

The SQLite3 database stores the source list, crawler queue, and content extracted
from pages. To create a database run:

```shell
sqlite3 data.db < schema.sql
```

## Running crawl

To run the crawl, run:

```shell
python crawler.py
```

While the crawl is running, details and diagnostic information is logged to
"crawl.log".
