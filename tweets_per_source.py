import sqlite3
import numpy as np


files = ["/data/NELA-GT-2018/nela-gt-2018.db", "/data/NELA-GT-2019/nela-gt-2019.db",
         "/data/NELA-GT-2020/nela-gt-2020.db"]

query = " SELECT d.source, count(*) as tweets, count(distinct article_id) as articles FROM " \
        " tweet t INNER JOIN newsdata d " \
        " ON t.article_id = d.id " \
        " GROUP BY source"


print(query)
tweet_count = dict()
for i, path in enumerate(files):
        con = sqlite3.connect(path)
        result = con.execute(query)
        for row in result.fetchall():
                if row[0] not in tweet_count:
                        tweet_count[row[0]] = np.zeros(2*len(files), dtype=np.int32)
                tweet_count[row[0]][i] = row[1]
                tweet_count[row[0]][i+len(files)] = row[2]

article_count = dict()
# Collects the total number of article per source for each input database.
query_articles = "SELECT source, count(id) as articles FROM newsdata GROUP BY source"
for i, path in enumerate(files):
        con = sqlite3.connect(path)
        result = con.execute(query_articles)
        for row in result.fetchall():
                if row[0] not in article_count:
                        article_count[row[0]] = np.zeros(len(files), dtype=np.int32)
                article_count[row[0]][i] = row[1]

for src in sorted(tweet_count):
        print(src, tweet_count[src])
path_out = "tweets-per-source.csv"
# Writes out CSV file with tweets, tweet_articles, and total articles for each input database.
with open(path_out, "w") as fout:
        fout.write("source,tweets_2018,tweets_2019,tweets_2020,tweet_articles_2018,tweet_articles_2019,"
                   "tweet_articles_2020,articles_2018,articles_2019,articles_2020\n")
        for src in sorted(tweet_count):
                fout.write("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d\n" % (src, tweet_count[src][0], tweet_count[src][1],
                                                                tweet_count[src][2], tweet_count[src][3],
                                                                tweet_count[src][4], tweet_count[src][5],
                                                                article_count[src][0], article_count[src][1],
                                                                article_count[src][2]))


"""
Queries the number of embedded tweet observed in each article of the database containing a tweet.
That is, this query is conditioned on an article containing an embedded tweet. Articles with no embedded
tweets are disregarded.
"""
query_tweets = " SELECT d.source, d.id, count(distinct t.article_id) as tweets FROM " \
                 " tweet t INNER JOIN newsdata d " \
                 " ON t.article_id = d.id " \
                 " GROUP BY d.id"

with open("avg-tweets.csv", "w") as fout:
    fout.write("source,article_id,tweets\n")
    for i, path in enumerate(files):
        con = sqlite3.connect(path)
        result = con.execute(query_tweets)
        for row in result.fetchall():
            fout.write("%s,%d,%d\n" % (row[0], row[1], row[2]))

