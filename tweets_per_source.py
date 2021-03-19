import sqlite3
import numpy as np


files = ["/data/NELA-GT-2018/nela-gt-2018.db", "/data/NELA-GT-2019/nela-gt-2019.db",
         "/data/NELA-GT-2020/nela-gt-2020.db"]

query = " SELECT d.source, count(t.id) as tweets, count(distinct article_id) as articles FROM " \
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

for src in sorted(tweet_count):
        print(src, tweet_count[src])
path_out = "tweets-per-source.csv"
with open(path_out, "w") as fout:
        fout.write("source,tweets_2018,tweets_2019,tweets_2020,articles_2018,articles_2019,articles_2020\n")
        for src in sorted(tweet_count):
                fout.write("%s,%d,%d,%d,%d,%d,%d\n" % (src, tweet_count[src][0], tweet_count[src][1],
                                                       tweet_count[src][2],
                                                       tweet_count[src][3], tweet_count[src][4], tweet_count[src][5]))
