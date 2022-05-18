import networkx as nx
from networkx.algorithms import community
import argparse
import sqlite3
import re
import numpy as np
import json
from collections import defaultdict
from sklearn.metrics import pairwise_distances


def load_user_data(path):
    """
    Load JSON file containing Twitter user data.
    :param path: Path to JSON file.
    :return user_data: JSON object indexed by username.
    """
    with open(path) as fin:
        user_data = json.load(fin)
    # for user in data:
    #     user_data[user["username"]] = user

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
        return "[UNKNOWN]"
    regex = re.compile("twitter.com/(?P<author>\w+)")
    result = regex.search(idx)

    if result:
        return result.groups()[0]
    return "[UNKNOWN]"


def get_tweet_authors(ids, return_counts=False):
    """
    Returns the authors for a given list of tweet URLs.
    :param ids: Tweet URLs.
    :param return_counts: If True, return value is a dictionary with no. of embedded tweets by each author.
    :return authors: Set of authors. If retorn_counts is True, returns a dictionary of author(str) -> count (int).
    """
    if return_counts:
        authors = dict()
    else:
        authors = set()
    regex = re.compile("twitter.com/(?P<author>\w+)/")

    for idx in ids:
        if idx is None:
            continue
        result = regex.search(idx)
        if result is not None:
            author = result.groups()[0]
            if return_counts:
                if author not in authors:
                    authors[author] = 0
                authors[author] += 1
            else:
                authors.add(author)
    return authors


def build_user_network(tweets, user_data, labels, p_threshold, min_links=5, exclude_authors={}):
    """
    Builds the network of source-tweet interaction.
    By default, it connects sources to twitter accounts based on whether a source embeds a tweet by that user.
    :param tweets: Rows of tweets containing embedded_tweet, article_id, source.
    :param user_data: JSON object containing Twitter user data.
    :param labels: Source credibility labels.
    :param p_threshold: Cutoff threshold for edges.
    :param min_links: Nodes with fewer than `min_link` will be removed from the network.
    :param exclude_authors: Usernames to exclude when building the network.
    :return g: NetworkX graph.
    """
    g = nx.Graph()
    edges = defaultdict(int)
    for t in tweets:
        _id = clean_tweet_id(t[2])
        t_author = get_tweet_author(_id)
        src = t[1]

        if t_author is None or t_author in exclude_authors:  # Skip this author
            continue
        if t_author in user_data:
            followers = int(user_data[t_author]["public_metrics"]["followers_count"])
            following = int(user_data[t_author]["public_metrics"]["following_count"])
            tweet_count = int(user_data[t_author]["public_metrics"]["tweet_count"])
        else:
            followers = 1
            following = 1
            tweet_count = 1

        g.add_node(t_author, class_="twitter", followers=followers, following=following, tweet_count=tweet_count)
        # Get node attributes
        if src in labels:
            cred = labels[src]
        else:
            cred = -1
        g.add_node(src, class_="news", credibility=cred)
        edges[(t_author, src)] += 1  # / (np.log(followers + 1e-6))

    if p_threshold is None:
        x = np.array(list(edges.values()))
        p_threshold = x.mean()

    for e in edges:
        if edges[e] > p_threshold:
            g.add_edge(e[0], e[1], weight=edges[e])

    to_remove = [node for node, degree in g.degree() if degree < min_links and g.nodes[node]["class_"] == "twitter"]
    g.remove_nodes_from(to_remove)

    return g


def build_source_network(tweets, user_data, labels, source_bias, p_threshold=None, exclude_authors={},
                         scaling=False,
                         alpha=1):
    """
    Builds a network of source-source relationships.
    Two nodes u, v represent sources that are connected if they share a common embedded tweet author.
    :param tweets: Tweets from NELA database.
    :param user_data: Dictionary keyed by username, containing user metadata (follower/following counts, etc.).
    :param labels: Source labels. These labels are saved as attributes of the source nodes.
    :param p_threshold: Edge weight cutoff. If `None`, use the distribution mean as cutoff. Low values generate denser networks.
    :param exclude_authors: (set) Authors to exclude when building network.
    :param scaling: If True, apply scaling to links based on the number of followers each user has.
    :param alpha: Multiplying factor for how many deviations above the mean to apply the cutoff (default=1). Low values generate denser networks.
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

        if t_author in exclude_authors:  # Skip this author
            continue

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
    for i, u in enumerate(sources.keys()):
        for j, v in enumerate(list(sources.keys())[i+1:]):
            common_refs = set.intersection(set(sources[u]), set(sources[v]))
            prob = 0
            for author in common_refs:
                if author in user_data and scaling is True:
                    scaling_factor = 1/(np.log(1e-6+user_data[author]["public_metrics"]["followers_count"]))
                else:
                    scaling_factor = 1
                prob += sources[u][author]*sources[v][author] * scaling_factor

            ebunch.append((u, v, prob))
            x.append(prob)
    print()
    x = np.array(x, dtype=np.float32)
    print("Mean edge weight(prob):", x.mean(), "+-", x.std())

    if p_threshold is None:
        p_threshold = x.mean() + alpha*x.std()

    e_bunch_filter = list()

    for e in ebunch:  # (u, v, weight) tuples
        if e[2] > p_threshold:
            e_bunch_filter.append(e)
    g.add_weighted_edges_from(e_bunch_filter)

    print("Setting node attributes...")
    for n in g.nodes:
        if n in labels:
            g.nodes[n]["bias"] = source_bias[n]
            g.nodes[n]["credibility"] = labels[n]
            g.nodes[n]["class"] = "news"
        else:
            g.nodes[n]["credibility"] = -1
            g.nodes[n]["class"] = "news"

    return g


def binary_overlap(x, y):
    """
    Returns how many common users are cited in vectors x and y.
    There is no weighting, x and y are matched whenever x[i] and y[i] are non-zero.
    :param x: d dimensional array.
    :param y:  d dimensiona array.
    :return: s (np.int32) number of matches in x & y.
    """
    return ((x != 0) & (y != 0)).astype(np.int32).sum()


def prob_overlap(x, y):
    """
    Returns the probability of overlap between vectors x and y, calculated as sum(xi * yi).
    :param x: Vector of probabilities.
    :param y: Vector of probabilities.
    :return: p - the overlap probability between x and y.
    """
    p = np.multiply(x, y).sum()
    return p


def jaccard_index(x, y):
    """
    Returns the jaccard index between two binary input vectors x and y.
    The jaccard index is computed by J = |x * y|/|x + y|.
    If `frequency` is given, each user i is weighted as 1/frequency[i].
    Where * denotes set intersection and + denotes set union.
    If x or y are not binary, the operation will be applied to its binarized versions.
    :param x: Input vector (size d).
    :param y: Input vector (size d).
    :return: J the jaccard index between x and y.
    """

    return ((x != 0) & (y != 0)).astype(np.float32).sum()/((x != 0) | (y != 0)).astype(np.float32).sum()


def get_overlap(x, y):
    """
    Returns a binary mask with the overlap between non-zero entries in x and y (common authors or sources).
    :param x: Input vector of size d.
    :param y: Input vector of size d.
    :return z: Binary mask of overlapping authors.
    """

    return (x != 0) & (y != 0)


def build_network(tweets, labels, source_bias, metric="overlap", nodes="sources",
                  min_count=0,
                  min_weight=0.1,
                  use_frequency=False):
    """
    Construct network with input tweets.
    :param tweets: Tweets from NELA database.
    :param labels: Dictionary of source credibility labels.
    :param source_bias: Dictionary of source bias ratings.
    :param metric: Metric to use when computing network edges.
    :param nodes: (str) Use "sources" for network of sources and "authors" for a network of Twitter users.
    :param min_count: (int) Remove nodes with fewer than `min_count` occurrences (discard row if sum < min_count).
    :param min_weight: (int) Remove edges whose weights are less than `min_weight`.
    :param use_frequency: (bool) If True, importance of links is measured by the inverse frequency of embedded tweets
                            for that user.
    :return: NetworkX graph g.
    """

    t_ids = [clean_tweet_id(t[2]) for t in tweets]
    authors = np.array(sorted({get_tweet_author(_id) for _id in t_ids}))
    sources = np.array(sorted({t[1] for t in tweets}))
    n_authors = len(authors)
    n_sources = len(sources)

    source_id = {s: i for i, s in enumerate(sources)}
    author_id = {a: i for i, a in enumerate(authors)}

    print("Sources: %d | Authors: %d" % (len(sources), len(authors)))

    m = np.zeros((n_sources, n_authors), dtype=np.int32)

    for t in tweets:
        _id = clean_tweet_id(t[2])
        _username = get_tweet_author(_id)
        if _username is None or _username == "[UNKNOWN]":
            continue
        i = source_id[t[1]]
        j = author_id[_username]
        m[i][j] += 1

    if nodes == "authors":  # Transpose matrix to compute user network.
        m = m.T
    bin_mask = ((m > 0).sum(axis=1)) >= min_count  # Count the number of non-zero entries in each row.
    m = m[bin_mask]  # Select only rows that have min_count non-zero entries.

    # Remove all-zero columns
    m = m[:, ((m > 0).sum(axis=0)) > 0]

    if use_frequency:
        # m = (m > 0).astype(np.int32) / (m_frequency ** (1/5))
        m_frequency = (m > 0).sum(axis=0) / m.shape[0]  # Compute the row frequency of each column
        print(authors)
        print(m_frequency)
        pass

    print("Distance matrix", m.shape)

    if metric == "overlap":
        m_adj = pairwise_distances(m, metric=binary_overlap).astype(np.int32)
    elif metric == "jaccard":
        m_adj = pairwise_distances(m, metric=jaccard_index)  # binary jaccard
    elif metric == "cosine":
        m_adj = pairwise_distances(m, metric="cosine")
        m_adj = 1 - m_adj  # Convert cosine distance to similarity
    elif metric == "inverse":
        m_adj = pairwise_distances(m, metric=prob_overlap)

    # Filter minimum edge weights
    m_adj = np.maximum(m_adj-min_weight, 0)
    np.fill_diagonal(m_adj, 0)  # Prevent self-loops
    g = nx.from_numpy_matrix(m_adj)

    if nodes == "sources":
        node_id = {s: i for i, s in enumerate(sources[bin_mask])}
        g = nx.relabel.relabel_nodes(g, {i: s for i, s in enumerate(sources[bin_mask])})
        print("Setting node attributes...")
        for n in g.nodes:
            if n in labels:
                g.nodes[n]["bias"] = source_bias[n]
                g.nodes[n]["credibility"] = labels[n]
                g.nodes[n]["class"] = "news"
            else:
                g.nodes[n]["credibility"] = -1
                g.nodes[n]["class"] = "news"
        print("Setting edge attributes...")
        # for u, v, a in g.edges(data=True):
        #     x = m[node_id[u]]
        #     y = m[node_id[v]]
        #     f_xy = (x+y)/2  # Get the user importance score between sources u and v
        #     sorted_ids = np.argsort(f_xy)[::-1]  # Sort user ids based on importance
        #     print(u, v, a, sorted(authors[sorted_ids][:5]))  # Get top 3 most important authors between u and v

    elif nodes == "authors":
        g = nx.relabel.relabel_nodes(g, {i: a for i, a in enumerate(authors[bin_mask])})

    return g


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="Path to NELA database")
    parser.add_argument("output", type=str, help="Path to output network.")
    parser.add_argument("--rowid", type=str, default=None, help="Path to csv file of rowids (to select articles).")
    parser.add_argument("--p_threshold", type=float, default=None, help="Cutoff threshold for edge weights.")
    parser.add_argument("--exclude_authors", type=str, default={}, nargs="+",
                        help="Authors to ignore when building the network.")
    parser.add_argument("--min_count", type=int, default=0, help="Min count parameter for build_network().")
    parser.add_argument("--min_weight", type=float, default=0, help="Edge weight cutoff.")
    parser.add_argument("--metric", choices=["overlap", "cosine", "jaccard", "inverse"], default="overlap",
                        help="Metric used when building the network.")
    parser.add_argument("--use_frequency", action="store_true", help="Use frequency as measure of importance.")
    parser.add_argument("--bipartite", action="store_true", help="Create graph with both source and twitter nodes.")
    parser.add_argument("--authors", action="store_true", help="Create network where nodes are authors.")
    parser.add_argument("--user-data", dest="user_data", default=None,
                        help="Path to user data JSON")
    args = parser.parse_args()

    path_user_data = args.user_data
    path_gml = args.output

    if args.rowid:
        row_ids = load_article_rowids(args.rowid)
        print("-- Topic articles:", len(row_ids))
    else:
        row_ids = None

    if args.user_data:
        user_data = load_user_data(path_user_data)
    else:
        user_data = {}

    print("-- User data", len(user_data))

    path_labels = "data/labels.csv"
    with open(path_labels) as fin:
        fin.readline()
        # labels = dict(map(lambda s: s.strip().split(","), fin.readlines()))
        labels = dict()
        source_bias = dict()
        for line in fin:
            source, country, label, bias, _ = line.strip().split(",", 4)
            labels[source] = int(label)
            source_bias[source] = bias

    con = sqlite3.connect(args.input)

    print("Loading tweets...")
    tweets = load_all_tweets(con, row_ids=row_ids)
    t_ids = [t[2] for t in tweets]
    t_authors = get_tweet_authors(t_ids, return_counts=True)

    # found = sum(author in user_data for author in t_authors)
    # print("Authors", found, len(t_authors))
    print("Loaded %d tweets from %d authors." % (len(tweets), len(t_authors)))

    if args.authors:
        g = build_network(tweets, labels, source_bias, args.metric,
                          nodes="authors",
                          min_count=args.min_count,
                          min_weight=args.min_weight,
                          use_frequency=args.use_frequency)
    else:
        g = build_network(tweets, labels, source_bias, args.metric,
                          min_count=args.min_count,
                          min_weight=args.min_weight,
                          use_frequency=args.use_frequency)

    print(len(g), "nodes", len(g.edges), "edges.")
    nx.write_gml(g, path_gml)
    print("Saved to %s" % path_gml)

    with open(path_gml.replace(".gml", ".csv"), "w") as fout:
         # Write ranking of most cited authors
        fout.write("author,embedded_tweets\n")
        for author in sorted(t_authors, key=lambda a: t_authors[a], reverse=True):
            fout.write("%s,%d\n" % (author, t_authors[author]))


if __name__ == "__main__":
    main()
