import sqlite3
from network import clean_tweet_id, get_tweet_author, load_user_data

path = "../data/nela/nela-gt-2020.db"
output = "twitter_info.csv"

user_data = load_user_data("user_data/user_data.json")

con = sqlite3.connect(path)
query = "SELECT t.rowid, t.embedded_tweet, d.source " \
        " FROM tweet t INNER JOIN newsdata d ON d.id = t.article_id"

data = con.execute(query)

with open(output, "w") as fout:
    fout.write("rowid,url,source,author,followers,following,tweet_count\n")
    for d in data:
        url = clean_tweet_id(d[1])
        author = get_tweet_author(url)
        source = d[2]
        if author in user_data:
            followers = user_data[author]["public_metrics"]["followers_count"]
            following = user_data[author]["public_metrics"]["following_count"]
            tweet_count = user_data[author]["public_metrics"]["tweet_count"]
        else:
            followers = ""
            following = ""
            tweet_count = ""

        fout.write("%s,%s,%s,%s,%s,%s,%s\n" % (d[0], url, source, author, followers, following, tweet_count))
