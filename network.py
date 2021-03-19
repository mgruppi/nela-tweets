import networkx as nx
import argparse
import sqlite3
import re
import numpy as np
import json


def load_user_data(path):
    """
    Load JSON file containing Twitter user data.
    :param path: Path to JSON file.
    :return user_data: JSON object indexed by username.
    """
    user_data = dict()
    with open(path) as fin:
        data = json.load(fin)

    for user in data:
        user_data[user["username"]] = user
    return user_data


def load_all_tweets(con, row_ids=None):
    """
    Load all data in table `tweet`. Results are ordered by `embedded_tweet` which corresponds to the tweets' URL.
    :param con: Sqlite3 connection to NELA database.
    :param row_ids: Article rowids to include. Only return tweets that appear in articles in row_ids.
    :return result: List of results, one tweet per row.
    """

    query = "SELECT t.article_id as article_id, a.source as source, embedded_tweet as url, a.rowid as rowid " \
            "FROM tweet t INNER JOIN newsdata a ON t.article_id = a.id"
    if row_ids is None:
        result = con.cursor().execute(query).fetchall()
    else:
        result = list()
        rid_set = set(row_ids)
        all_results = con.cursor().execute(query).fetchall()
        for i, r in enumerate(all_results):
            if str(r[-1]) in rid_set:
                result.append(r)
    return result


def load_article_rowids(path):
    """
    Load and returns article row ids for a given topic.
    :param path: Path to file.
    :return row_ids: List of rowids, this field can be used to query a sqlite3 database.
    """
    with open(path) as fin:
        fin.readline()  # read header
        data = map(lambda s: s.strip().split(",", 2), fin.readlines())
        row_ids, months, sources = zip(*data)

    return row_ids


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


def build_source_network(tweets, user_data, p_threshold=None):
    """
    Builds a network of source-source relationships.
    Two nodes u, v represent sources that are connected if they share a common embedded tweet author.
    :param tweets: Tweet data.
    :param user_data: Dictionary keyed by username, containing user information.
    :param p_threshold: Edge weight cutoff. If `None`, use the distribution mean as cutoff.
    :return: g NetworkX undirected graph.
    """

    # We will begin by constructing a dictionary mapping author -> source -> weight
    # weight is the number of times a source cites an author
    authors = dict()  # dict that stores author -> source -> no. of citations
    sources = dict()  # dict that stores source -> author -> no. of citations

    for t in tweets:
        _id = clean_tweet_id(t[2])
        t_author = get_tweet_author(_id)
        src = t[1]
        if t_author not in authors:
            authors[t_author] = dict()
        if src not in authors[t_author]:
            authors[t_author][src] = 0
        if src not in sources:
            sources[src] = dict()
        if t_author not in sources[src]:
            sources[src][t_author] = 0

        authors[t_author][src] += 1
        sources[src][t_author] += 1

    # We will compute the main (u, v) edge weight as the probability that, given a set of common references S,
    # and given that u and v will cite, the u and v cite a common source in S.
    # We will treat the citations by u and v of a reference A as independent events.

    for src in sources:
        num_references = sum(sources[src].values())
        for a in sources[src]:  # compute probability that src cites a.
            sources[src][a] = sources[src][a]/num_references

    g = nx.Graph()
    x = list()  # store edge weight distribution.
    ebunch = list()  # store edge (u, v, weight) tuples.
    # Iterate over each pair of sources in `sources`.
    num_pairs = len(sources)**2
    for i, u in enumerate(sources.keys()):
        for j, v in enumerate(list(sources.keys())[i+1:]):
            print("Computing edge weights (%2.2f)" % (100*(i*len(sources)+j+1)/num_pairs), end="\r")
            common_refs = set.intersection(set(sources[u]), set(sources[v]))
            prob = 0
            for author in common_refs:
                if author in user_data:
                    scaling_factor = 1/(1e-5+np.log(user_data[author]["public_metrics"]["followers_count"]))
                else:
                    scaling_factor = 1
                prob += sources[u][author]*sources[v][author] * scaling_factor

            ebunch.append((u, v, prob))
            x.append(prob)
    print()
    x = np.array(x, dtype=np.float32)
    print("Mean edge weight(prob):", x.mean(), "+-", x.std())

    if p_threshold is None:
        p_threshold = x.mean()

    e_bunch_filter = list()

    for e in ebunch:  # (u, v, weight) tuples
        if e[2] > p_threshold:
            e_bunch_filter.append(e)
    g.add_weighted_edges_from(e_bunch_filter)

    return g


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=str, help="Path to output network.")
    parser.add_argument("--rowid", type=str, default=None, help="Path to csv file of rowids (to select articles).")
    parser.add_argument("--p_threshold", type=float, default=None, help="Cutoff threshold for edge weights.")
    args = parser.parse_args()

    path_user_data = "user_data/user_data.json"
    path_gml = args.output

    if args.rowid:
        row_ids = load_article_rowids(args.rowid)
        print("-- Topic articles:", len(row_ids))
    else:
        row_ids = None

    user_data = load_user_data(path_user_data)

    print("-- User data", len(user_data))

    path_labels = "data/labels.csv"
    with open(path_labels) as fin:
        fin.readline()
        labels = dict(map(lambda s: s.strip().split(","), fin.readlines()))

    path = "../data/dataverse/release/nela-gt-2020.db"
    con = sqlite3.connect(path)

    tweets = load_all_tweets(con, row_ids=row_ids)
    t_ids = [t[2] for t in tweets]
    t_authors = get_tweet_authors(t_ids)

    found = sum(author in user_data for author in t_authors)

    print("Authors", found, len(t_authors))
    print("Loaded %d tweets from %d authors." % (len(tweets), len(t_authors)))

    # g = build_network(tweets, cutoff=5)
    g = build_source_network(tweets, user_data, p_threshold=args.p_threshold)

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
        else:
            g.nodes[n]["credibility"] = "unlabeled"
            g.nodes[n]["class"] = "news"
            g.nodes[n]["cred"] = 0.0

    print(len(g), "nodes", len(g.edges), "edges.")
    nx.write_gml(g, path_gml)
    print("Saved to %s" % path_gml)


if __name__ == "__main__":
    main()
