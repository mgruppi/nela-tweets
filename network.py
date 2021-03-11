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


def build_network(tweets, cutoff=1):
    """
    Builds the network of source-tweet interaction.
    By default, it connects sources to twitter accounts based on whether a source embeds a tweet by that user.
    :param tweets: Rows of tweets containing embedded_tweet, article_id, source.
    :param cutoff: Cutoff point for edge weights. Only include edges whose weight is > `min_count`.
    :return g: NetworkX graph.
    """
    g = nx.Graph()
    weights = dict()
    for t in tweets:
        _id = clean_tweet_id(t[2])
        tgt = get_tweet_author(_id)  # Target (tweet author)
        src = t[1]  # Source (news source name)
        if src is None or tgt is None:
            # print("ERROR", _id, src, tgt)
            continue

        if (src, tgt) not in weights:
            weights[(src, tgt)] = 0
        weights[(src, tgt)] += 1

    # Prepare edge bunch. This is a 3-tuple consisting of (src, tgt, w) where w is the edge weight.
    ebunch = list()
    for e in weights:
        if weights[e] > cutoff:
            ebunch.append((e[0], e[1], weights[e]))
    g.add_weighted_edges_from(ebunch)

    return g


def build_source_network(tweets, cutoff=1):
    """
    Builds a network of source-source relationships.
    Two nodes u, v represent sources that are connected if they share a common embedded tweet author.
    :param tweets:
    :param cutoff: Cutoff value for edge weights. Only weights > `cutoff` will be included as edges.
    :return: g NetworkX undirected graph.
    """

    # We will begin by constructing a dictionary mapping author -> source -> weight
    authors = dict()

    for t in tweets:
        _id = clean_tweet_id(t[2])
        t_author = get_tweet_author(_id)
        src = t[1]
        if t_author not in authors:
            authors[t_author] = dict()
        if src not in authors[t_author]:
            authors[t_author][src] = 0

        authors[t_author][src] += 1
    ebunch = list()

    g = nx.Graph()
    for a in authors:
        for i, src_a in enumerate(authors[a]):
            for j, src_b in enumerate(list(authors[a].keys())[i+1:]):  # create pairwise links
                g.add_edge(src_a, src_b, weight_a=authors[a][src_a], weight_b=authors[a][src_b])
    return g


def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    path_gml = "graph.gml"

    path_labels = "data/labels.csv"
    with open(path_labels) as fin:
        fin.readline()
        labels = dict(map(lambda s: s.strip().split(","), fin.readlines()))

    path = "../data/dataverse/release/nela-gt-2020.db"
    con = sqlite3.connect(path)

    tweets = load_all_tweets(con)
    article_ids = get_unique_articles(con)

    t_ids = [t[2] for t in tweets]
    t_authors = get_tweet_authors(t_ids)

    print("Loaded %d tweets from %d authors." % (len(tweets), len(t_authors)))
    print("Found %d article ids." % len(article_ids))

    # g = build_network(tweets, cutoff=5)
    g = build_source_network(tweets, cutoff=5)

    print("Setting node attributes...")
    for n in g.nodes:
        if n in labels:
            g.nodes[n]["credibility"] = labels[n]
            g.nodes[n]["class"] = "news"
            if labels[n] == "0":
                g.nodes[n]["cred"] = 1.0
            elif labels[n] == "1" or labels[n] == "2":
                g.nodes[n]["cred"] = -1.0
            else:
                g.nodes[n]["cred"] = 0.0
        # elif n in t_authors:
        #     g.nodes[n]["credibility"] = "N/A"
        #     g.nodes[n]["class"] = "twitter"
        #     g.nodes[n]["cred"] = 0.0
        #     for edge in g.edges(n):
        #         if edge[1] in labels:
        #             if labels[edge[1]] == "0":
        #                 g.nodes[n]["cred"] += 1.
        #             elif labels[edge[1]] == "1" or labels[edge[1]] == "2":
        #                 g.nodes[n]["cred"] += -1.
        #     g.nodes[n]["cred"] = g.nodes[n]["cred"]/len(g.edges(n))
        else:
            g.nodes[n]["credibility"] = "unlabeled"
            g.nodes[n]["class"] = "news"
            g.nodes[n]["cred"] = 0.0

    print(len(g), "nodes", len(g.edges), "edges.")
    nx.write_gml(g, path_gml)
    print("Saved to %s" % path_gml)


if __name__ == "__main__":
    main()
