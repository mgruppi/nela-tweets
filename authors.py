"""
Generate data from the authors of the tweets.
Combining information from the news source that embeds the author, date, followers and following counts.
"""

import json
import sqlite3
import network as nt
import pandas as pd


path_user_data = "user_data/user_data.json"
path_db = "../data/nela/nela-gt-2020.db"

with open(path_user_data) as fin:
    user_data = json.load(fin)


con = sqlite3.connect(path_db)
tweets = nt.load_all_tweets(con)

tweets_mod = list()
for i, t in enumerate(tweets):
    author = nt.get_tweet_author(t[2])
    if author in user_data:
        followers = user_data[author]["public_metrics"]["followers_count"]
        following = user_data[author]["public_metrics"]["following_count"]
    else:
        followers = -1
        following = -1
    tweets_mod.append(list(t) + [author, followers, following])

df = pd.DataFrame(tweets_mod, columns=["article_id", "source", "url", "rowid", "author", "followers", "following"])
print(df)

df.to_csv("data/author-source.csv")

