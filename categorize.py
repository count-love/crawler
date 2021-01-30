"""
Copyright 2021, Count Love Crawler

Categorization code implements an algorithm for sorting similar articles
together. It achieves this by building a distance matrix between articles
and uses a traveling salesperson style algorithm to find the shortest
route between all articles.

Sorting can be directly invoked by running this file:

> python categorize.py
"""

import hashlib
import itertools
import logging
import math
import numpy as np
import re

import common
from paragraphs import generate_paragraphs_from_text

SIGNATURE_HASH_BUCKETS = 1000000


def get_text_shingles(text, default_shingle_length=8):
    for i in range(0, 1 + len(text) - default_shingle_length):
        shingle = text[i:(i + default_shingle_length)]
        yield shingle


def calculate_mini_signatures(signature, rows_per_band=10000):
    miniSignature = []
    signatureLength = len(signature)

    for start in range(0, signatureLength, rows_per_band):
        end = start + rows_per_band
        if np.any(signature[start:end]):
            band = signature[start:end].tobytes()
            hashedBand = hashlib.md5(band).hexdigest()
            miniSignature.append(hashedBand)
        else:
            miniSignature.append(0)

    return miniSignature


def calculate_signature(text):
    signature = np.zeros((SIGNATURE_HASH_BUCKETS, ), dtype=np.bool_)

    for shingle in set(get_text_shingles(text)):
        # run once per shingle, to minimize hashing
        index = int(hashlib.md5(shingle.encode('utf-8')).hexdigest(), 16) % SIGNATURE_HASH_BUCKETS
        signature[index] = True

    return signature


def compare_signatures(x, y):
    intersection = np.bitwise_and(x, y).sum()
    union = np.bitwise_or(x, y).sum()

    try:
        jaccard = float(intersection) / float(union)
    except ZeroDivisionError:
        return 0

    return jaccard


def calculate_express_signature(text):
    ret = set()

    for shingle in set(get_text_shingles(text)):
        # run once per shingle, to minimize hashing
        index = int(hashlib.md5(shingle.encode('utf-8')).hexdigest(), 16) % SIGNATURE_HASH_BUCKETS
        ret.add(index)

    return ret


def compare_express_signatures(x, y):
    z = x.intersection(y)
    len_z = len(z)
    # union is the length of each signature minus the length of the intersection
    return float(len_z) / (len(x) + len(y) - len_z)


def compare_mini_signatures(x, y):

    for index, signature in enumerate(x):
        if x[index] == y[index] and x[index] != 0:
            return True

    return False


re_strip_headings = re.compile(r'^#+ ')  # strip markdown headings
re_strip_format = re.compile(r' \*{1,2}([^\*]+)\*{1,2} ')  # strip markdown bold and italic
re_strip_links = re.compile(r'\[([^\]]+)\]\(https?://[^)]+\)', flags=re.IGNORECASE)  # strip markdown links'
re_non_letters = re.compile(r'[^a-zA-Z]*')


def clean_text(text):
    output = ''
    for paragraph in generate_paragraphs_from_text(text):
        # clean markdown
        text = re_strip_headings.sub('', paragraph)
        text = re_strip_format.sub(r' \1 ', text)
        text = re_strip_links.sub(r'\1', text)

        # delete anything that is not a letter
        output = output + re_non_letters.sub('', text)

    return output


def sort_articles_by_similarity(db_conn, db_cursor):
    def path_distance(route, distance):
        total_distance = 1e-7 # epsilon

        # map indices to article IDs
        for stop_index, node in enumerate(route[0:-1]):
            a = route[stop_index]
            b = route[stop_index+1]

            total_distance += distance[a][b]

        return total_distance

    # Reverse the order of all elements from element i to element k in array r.
    two_opt_swap = lambda r, i, k: np.concatenate((r[0:i], r[k:-len(r) + i - 1:-1], r[k + 1:len(r)]))

    # https://stackoverflow.com/questions/25585401/travelling-salesman-in-scipy
    def two_opt(nodes, distance, improvement_threshold):  # 2-opt Algorithm adapted from https://en.wikipedia.org/wiki/2-opt
        route = np.arange(len(nodes))  # Make an array of row numbers corresponding to nodes.
        improvement_factor = 1  # Initialize the improvement factor.
        best_distance = path_distance(route, distance)  # Calculate the distance of the initial path.
        while improvement_factor > improvement_threshold:  # If the route is still improving, keep going!
            distance_to_beat = best_distance  # Record the distance at the beginning of the loop.
            for swap_first in range(1, len(route) - 2):  # From each city except the first and last,
                for swap_last in range(swap_first + 1, len(route)):  # to each of the nodes following,
                    new_route = two_opt_swap(route, swap_first, swap_last)  # try reversing the order of these nodes
                    new_distance = path_distance(new_route, distance)  # and check the total distance with this modification.
                    if new_distance < best_distance:  # If the path distance is an improvement,
                        route = new_route  # make this the accepted best route
                        best_distance = new_distance  # and update the distance corresponding to this route.
            improvement_factor = 1 - best_distance / distance_to_beat  # Calculate how much the route has improved.

        return route  # When the route is no longer improving substantially, stop searching and return the route.

    # get unreviewed and unsorted articles
    logging.debug('Fetching crawler content for sorting')
    db_cursor.execute('SELECT cq.ID AS QueueID, cc.Text AS Text FROM CrawlerQueue AS cq, CrawlerContent AS cc '
                      'WHERE cq.Category IS NULL AND '
                      'cc.QueueID = cq.ID AND '
                      'cc.Text IS NOT NULL '
                      'AND LENGTH(cc.Text) > 70 '
                      'ORDER BY cq.ID ASC')

    unreviewed_articles = db_cursor.fetchall()
    unreviewed_articles_ids = [x['QueueID'] for x in unreviewed_articles]
    logging.debug('Found %d articles', len(unreviewed_articles_ids))

    distance = np.zeros((len(unreviewed_articles_ids), len(unreviewed_articles_ids)))

    # for all pairwise combinations of article IDs...
    for comparison in itertools.combinations(unreviewed_articles_ids, 2):

        index_a = comparison[0]
        index_b = comparison[1]

        text = next(x for x in unreviewed_articles if x['QueueID'] == index_a)['Text']
        signature_a = calculate_signature(clean_text(text))

        text = next(x for x in unreviewed_articles if x['QueueID'] == index_b)['Text']
        signature_b = calculate_signature(clean_text(text))

        try:
            similarity = compare_signatures(signature_a, signature_b)

            matrix_index_a = unreviewed_articles_ids.index(index_a)
            matrix_index_b = unreviewed_articles_ids.index(index_b)
            distance[matrix_index_a][matrix_index_b] = 1 - similarity
            distance[matrix_index_b][matrix_index_a] = 1 - similarity

        # if there is no text, we'll give a division by zero error from the compare_signatures function
        except ZeroDivisionError:
            logging.error('Division by zero while sorting pair: (%d, %d)', index_a, index_b)

    logging.info('Calculated distance matrix for sorting articles')

    # Find a good route with 2-opt ("route" gives the order in which to travel to each city by row number.)
    route = two_opt(unreviewed_articles_ids, distance, 0.0001)
    logging.info('Calculated optimal path for sorting articles')

    to_update = []

    article_offset = 0
    db_cursor.execute('SELECT MAX(Category) AS SortIndex FROM CrawlerQueue '
                      'WHERE Category != \'notext\' AND Category IS NOT NULL')
    result = db_cursor.fetchone()
    if result is not None:
        article_offset = int(result['SortIndex'] or '0') + 1

    digits = 1 + int(math.floor(math.log10(article_offset + len(route))))

    previous_article_index = None
    for index, article_index in enumerate(route):
        article = next(x for x in unreviewed_articles if x['QueueID'] == unreviewed_articles_ids[article_index])

        if previous_article_index is None:
            logging.debug('%d, %d, %s', index, article['QueueID'], ''.join(article['Text'][0:160].splitlines()))
        else:
            a_b_distance = distance[previous_article_index][article_index]
            logging.debug('%f, %d, %d, %s',
                          a_b_distance,
                          index,
                          article['QueueID'],
                          ''.join(article['Text'][0:160].splitlines()))

        previous_article_index = article_index

        category = '%%0%dd' % digits % (index + article_offset)
        to_update.append((category, article['QueueID']))

    # set category for duplicates
    db_cursor.executemany('UPDATE CrawlerQueue SET Category = ? WHERE ID = ?', to_update)
    db_conn.commit()

    # set category for articles with no text
    db_cursor.execute('UPDATE CrawlerQueue SET Category=\'notext\' '
                      'WHERE Category IS NULL')
    db_conn.commit()

    logging.debug('Saved article sort order')


def main():
    # connect to database
    db_connection = common.get_db_connection(local=True)
    db_cursor = db_connection.cursor()

    # assign mini signatures to articles and group articles together
    sort_articles_by_similarity(db_connection, db_cursor)

    # close connection
    db_cursor.close()
    db_connection.close()


if __name__ == "__main__":
    main()
