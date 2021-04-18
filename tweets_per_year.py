import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

"""
Plots figures showing number of Tweets per year (2018, 2019, 2020) and per source.
"""


def make_bar_chart(xticklabels, series, facecolors=None, labels=None, width=0.25,
                   xlabel = "Year",
                   ylabel = "Avg. Articles w/ Tweets (%)",
                   xlim=None, ylim=None):
    """
    Creates a grouped bar chart using Pyplot.
    :param indices: Name of the indices (horizontal axis).
    :param series: List of series (data) (vertical axis).
    :param labels: Labels for each series.
    :param width: (float) Width of each bar.
    :param xlim: X-axis limits (lower, upper).
    :param ylim: Y-axis limits (lower, upper).
    :return: fig, ax - Pyplot figure and axes.
    """

    x = np.arange(len(xticklabels))
    fig, ax = plt.subplots()
    ax.grid(axis='y', linestyle="dashed")
    for i, s in enumerate(series):
        ax.bar(x - (1-i)*(width/2) + (i)*(width/2), s, width=width,
               facecolor=facecolors[i] if facecolors else None,
               label=labels[i] if labels else None)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(xticklabels)

    if labels is not None:
        ax.legend(loc="best", ncol=2)
    if xlim:
        ax.set_xlim(xlim)
    if ylim:
        ax.set_ylim(ylim)
    fig.tight_layout()
    return fig, ax


drop_zeros = True  # Drop rows that contains a zero in either of tweets_2018, tweets_2019, tweets_2020.
split_by_label = True  # Split number of articles/tweets by credibility labels.


labels_file = "../data/nela/labels.csv"

with open(labels_file) as fin:
    fin.readline()
    data = dict(map(lambda s: s.strip().split(","), fin.readlines()))

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

if drop_zeros:
    df = df[(df["tweets_2018"] != 0) & (df["tweets_2019"] != 0) & (df["tweets_2020"] != 0)]

print(df)

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


num_articles_rel = np.array([df_rel["articles_2018"].sum(), df_rel["articles_2019"].sum(),
                            df_rel["articles_2020"].sum()], dtype=np.int)
tweet_articles_rel = np.array([df_rel["tweet_articles_2018"].sum(), df_rel["tweet_articles_2019"].sum(),
                              df_rel["tweet_articles_2020"].sum()], dtype=np.int)

num_articles_unr = np.array([df_unr["articles_2018"].sum(), df_unr["articles_2019"].sum(),
                            df_unr["articles_2020"].sum()], dtype=np.int)
tweet_articles_unr = np.array([df_unr["tweet_articles_2018"].sum(), df_unr["tweet_articles_2019"].sum(),
                              df_unr["tweet_articles_2020"].sum()], dtype=np.int)


fig, ax = plt.subplots()

colors = ["#2f4b7c", "#f95d6a"]
labels = ["Reliable", "Unreliable"]

# Plot tweets per source
ax.bar(["2018", "2019", "2020"], num_articles, facecolor="#003f5c", label="Number of articles", width=0.3)
ax.bar(["2018", "2019", "2020"], tweets, facecolor="#ffa600", label="Number of tweets", width=0.3)
ax.grid(axis="y", linestyle="dashed")
ax.set_xlabel("Year")
ax.set_ylabel("Embedded tweets")
plt.legend()
plt.tight_layout()
fig.savefig("results/tweets-per-source.pdf", format="pdf")

fig, ax = make_bar_chart(["2018", "2019", "2020"],
                         [100*tweet_articles_rel/num_articles_rel, 100*tweet_articles_unr/num_articles_unr],
                         facecolors=colors, labels=labels,
                         ylim=(0, 31))
fig.savefig("results/avg-articles-tweets.pdf", format="pdf")


