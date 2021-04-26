# Get articles for a given embedded tweet.
# Used for qualitative analysis and comparison of narratives between reliable and unreliable sources citing a tweet.

import sqlite3
from network import load_all_tweets, clean_tweet_id

tweet_url = "https://twitter.com/WHO/status/1217043229427761152?ref_src=twsrc%5Etfw"

database = "../data/nela/nela-gt-2020.db"

con = sqlite3.connect(database)

labels_file = "data/labels.csv"
with open(labels_file) as fin:
    fin.readline()
    labels = dict()
    for line in fin:
        source, country, label, bias, _ = line.strip().split(",", 4)
        labels[source] = int(label)

clean_url = clean_tweet_id(tweet_url)
query = "SELECT d.source, d.title, d.url FROM " \
        " tweet t INNER JOIN newsdata d " \
        " ON t.article_id = d.id " \
        " WHERE t.embedded_tweet = ?"

results = con.execute(query, (tweet_url,)).fetchall()
print(len(results), "results")

results2 = con.execute(query, (clean_url,)).fetchall()

print("-------")
print("UNRELIABLE")
for r in results + results2:
    if r[0] in labels and labels[r[0]] == 1:
        print(r)

print("------")
print("RELIABLE")
for r in results + results2:
    if r[0] in labels and labels[r[0]] == 0:
        print(r)

#
# query = "SELECT count(distinct t.article_id) as n_articles, count(distinct d.source) as n_sources, t.embedded_tweet " \
#         " FROM tweet t INNER JOIN newsdata d ON " \
#         " t.article_id = d.id " \
#         " GROUP BY t.embedded_tweet " \
#         " ORDER BY n_sources DESC " \
#         " LIMIT 100"
# results = con.execute(query).fetchall()
#
# print(*results, sep="\n")

print(len(results) + len(results2))