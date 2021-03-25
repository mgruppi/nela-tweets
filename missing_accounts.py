"""
Check which Twitter user accounts are not in the given user_data.json file.
"""

from network import load_all_tweets, get_tweet_authors, load_user_data
from twitter_api import TwitterAPI
import sqlite3
import json
import os


path_data = "user_data/user_data.json"
path_db = "../data/nela/nela-gt-2020.db"

with open("oauth2.key") as fin:
    credentials = json.load(fin)

# Setup
api = TwitterAPI(credentials=credentials)
user_data = load_user_data(path_data)
con = sqlite3.connect(path_db)
tweets = load_all_tweets(con)

# Get tweets and authors
t_ids = [t[2] for t in tweets]
t_authors = get_tweet_authors(t_ids)

# Find missing authors
missing_authors = list()
for author in t_authors:
    if author not in user_data:
        missing_authors.append(author)

# Prepare to re-collect authors
data = dict()

if not os.path.exists("missing_accounts"):
    os.mkdir("missing_accounts")

data = api.get_users_batch(a, path_out="missing_accounts", return_any=True)
