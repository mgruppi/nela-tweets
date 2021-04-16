import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np

"""
Plots figures showing number of Tweets per year (2018, 2019, 2020) and per source.
"""

font = {
    "family": "Liberation Sans",
    "weight": "normal",
    "size": 14
}
matplotlib.rc('font', **font)

with open("data/labels.csv") as fin:
    fin.readline()  # remove header
    labels = dict(map(lambda s: s.strip().split(","), fin.readlines()))

df = pd.read_csv("data/tweets-per-source.csv")


tweets = np.array([df["tweets_2018"].sum(), df["tweets_2019"].sum(), df["tweets_2020"].sum()], dtype=np.int)
tweet_articles = np.array([df["tweet_articles_2018"].sum(), df["tweet_articles_2019"].sum(),
                           df["tweet_articles_2020"].sum()], dtype=np.int)
num_articles = np.array([df["articles_2018"].sum(), df["articles_2019"].sum(),
                         df["articles_2020"].sum()], dtype=np.int)

print("- Tweets | Articles")
print("   + 2018:", df["tweets_2018"].sum(), df["articles_2018"].sum(), df["tweet_articles_2018"].sum())
print("   + 2019:", df["tweets_2019"].sum(), df["articles_2019"].sum(), df["tweet_articles_2019"].sum())
print("   + 2020:", df["tweets_2020"].sum(), df["articles_2020"].sum(), df["tweet_articles_2020"].sum())


sources_reliable = [s for s in labels if labels[s] == "0"]
sources_unreliable = [s for s in labels if labels[s] == "1"]

df_rel = df[(df["source"].isin(sources_reliable))]
df_unr = df[(df["source"].isin(sources_unreliable))]

fig, ax = plt.subplots()
ax.bar(["2018", "2019", "2020"], num_articles, facecolor="#003f5c", label="Number of articles")
ax.bar(["2018", "2019", "2020"], tweets, facecolor="#ffa600", label="Number of tweets")
ax.set_xlabel("Year")
ax.set_ylabel("Embedded tweets")
plt.legend()
plt.tight_layout()
fig.savefig("tweets-per-source.pdf", format="pdf")


