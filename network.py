import networkx as nx
import argparse
import sqlite3
import re


def load_all_tweets(con):
    """
    Load all data in table `tweet`. Results are ordered by `embedded_tweet` which corresponds to the tweets' URL.
    :param con: Sqlite3 connection to NELA database.
    :return result: List of results, one tweet per row.
    """
    query = "SELECT t.article_id as article_id, a.source as source, embedded_tweet as url " \
            "FROM tweet t INNER JOIN newsdata a ON t.article_id = a.id"
    result = con.cursor().execute(query).fetchall()
    return result


def clean_tweet_id(_id):
    """
    Cleans up an embedded_tweet id by removing URL arguments (after ?) to ensure IDs are normalized.
    Splits the URL on ? and return the leftmost part of the string.
    :param _id: ID of the embedded tweet (URL).
    :return _id_r: The cleaned-up version of the input ID.
    """
    if _id is None:
        return None
    r = _id.split("?ref_src")[0]
    return r


def get_unique_articles(con):
    """
    Get the number of unique articles among the embedded tweets.
    :param con: Sqlite3 connection to NELA database.
    :return result: List of results, one article_id per row.
    """
    query = "SELECT distinct (article_id) FROM tweet"
    result = con.cursor().execute(query).fetchall()
    return result


def get_tweet_author(idx):
    """
    Retrieves the author of a given tweet.
    Uses regex matching to extract the username from a tweet URL.
    :param idx: Tweet URL.
    :return author: Username of the tweet author. Returns `None` if it fails to retrieve the username.
    """
    if idx is None:
        return None
    regex = re.compile("twitter.com/(?P<author>\w+)")
    result = regex.search(idx)

    if result:
        return result.groups()[0]
    return None


def get_tweet_authors(ids):
    """
    Returns the authors for a given list of tweet URLs.
    :param ids: Tweet URLs.
    :return authors: Set of authors.
    """
    authors = set()
    regex = re.compile("twitter.com/(?P<author>\w+)/")

    for idx in ids:
        if idx is None:
            continue
        result = regex.search(idx)
        if result is not None:
            author = result.groups()[0]
            authors.add(author)
    return authors


def build_network(tweets):
    """
    Builds the network of source-tweet interaction.
    By default, it connects sources to twitter accounts based on whether a source embeds a tweet by that user.
    :param tweets: Rows of tweets containing embedded_tweet, article_id, source.
    :return g: NetworkX graph.
    """
    g = nx.Graph()
    for t in tweets:
        _id = clean_tweet_id(t[2])
        src = get_tweet_author(_id)  # Source (tweet author)
        tgt = t[1]  # Target (news source name)
        if src is None or tgt is None:
            # print("ERROR", _id, src, tgt)
            continue
        g.add_edge(src, tgt)

    return g


def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    path = "../data/dataverse/release/nela-gt-2020.db"
    con = sqlite3.connect(path)

    tweets = load_all_tweets(con)
    article_ids = get_unique_articles(con)

    t_ids = [t[2] for t in tweets]
    t_authors = get_tweet_authors(t_ids)

    print("Loaded %d tweets from %d authors." % (len(tweets), len(t_authors)))
    print("Found %d article ids." % len(article_ids))

    g = build_network(tweets)

    print(len(g))
    nx.write_gml(g, "graph.gml")


if __name__ == "__main__":
    main()
