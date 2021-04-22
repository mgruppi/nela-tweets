import matplotlib.pyplot as plt
import matplotlib
import seaborn as sb
import pandas as pd
import os
import json
import numpy as np
from scipy.stats import spearmanr, pearsonr
from datetime import datetime

"""
Analyze file 'author-source.csv' to investigate the relationships between authors cited by sources and their respective
profiles, such as number of followers and number of following (aka friends).
"""


def get_account_age(user_dict):
    """
    Returns the number of months between the accounts `created_at` date and now.
    :param user_dict: Dictionary of user data.
    :return: n - Age of the account (in months).
    """
    t = datetime.strptime(user_dict["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
    age = (datetime.now() - t).days
    return age


font = {
    "family": "Liberation Sans",
    "weight": "normal",
    "size": 14
}
matplotlib.rc('font', **font)

labels_file = "../../data/nela/labels.csv"
data_file = "../data/author-source.csv"
user_data_file = "../user_data/user_data.json"

with open(user_data_file) as fin:
    user_data = json.load(fin)

exclude_authors = {"realDonaldTrump"}

plot_follower_distributions = True
plot_age_distribution = True

with open(labels_file) as fin:
    fin.readline()  # remove header
    labels = dict(map(lambda s: s.strip().split(","), fin.readlines()))

df = pd.read_csv(data_file)
series_l = pd.Series([int(labels[s]) if s in labels else -1 for s in df["source"]])
df["label"] = series_l

print(df.columns)

tweet_count = df["source"].value_counts()
sources_in = set([s for s in tweet_count.index if tweet_count[s] > 5])

# Filter dataframes, only include followers > 0
df_rel = df[(df["label"] == 0) & (df["followers"] > 0) & (df["source"].isin(sources_in))]
df_unr = df[(df["label"] == 1) & (df["followers"] > 0) & (df["source"].isin(sources_in))]

df_rel = df_rel.drop_duplicates(["source", "author"])
df_unr = df_unr.drop_duplicates(["source", "author"])

# Plot follower distros
if plot_follower_distributions:

    sb.histplot(df_rel["followers"]+1e-5, log_scale=True, stat="probability", label="Reliable", color="#2f4b7c",
                linewidth=0)
    sb.histplot(df_unr["followers"]+1e-5, log_scale=True, stat="probability", label="Unreliable", color="#f95d6a",
                linewidth=0)
    plt.xlabel("# Followers (of cited users)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("../results/cited-followers-dist.pdf", format="pdf")

    plt.clf()

    for root, dirs, files in os.walk("../topics/0.5_april_20"):
        for f in files:
            with open(os.path.join(root, f)) as fin:
                fig, ax = plt.subplots()
                fin.readline()  # read header
                rowids, month, source = zip(*map(lambda s: s.strip().split(","), fin.readlines()))
                rowids = set(rowids)
                x_rel = df_rel[df_rel["rowid"].isin(rowids)]
                x_unr = df_unr[df_unr["rowid"].isin(rowids)]
                sb.histplot(x_rel["followers"]+1e-5, log_scale=True, stat="probability", label="Reliable",
                            color="#2f4b7c", linewidth=0)
                sb.histplot(x_unr["followers"] + 1e-5, log_scale=True, stat="probability", label="Unreliable",
                            color="#f95d6a", linewidth=0)
                ax.legend()
                fig.savefig("../results/cited-followers-%s.pdf" % f.split(".")[0], format="pdf")
        plt.clf()

    # Plot distributions of tweet counts per source

    x = df_rel["source"].value_counts()
    z = df_unr["source"].value_counts()

    sb.histplot(x, log_scale=True, stat="probability", label="Reliable", color="#2f4b7c")
    sb.histplot(z, log_scale=True, stat="probability", label="Unreliable", color="#f95d6a")
    plt.savefig("../results/source-dist.pdf", format="pdf")
    plt.clf()

    authors_rel = set(df_rel["author"].unique())
    authors_unr = set(df_unr["author"].unique())
    authors_common = set.intersection(authors_rel, authors_unr)

    x = df_rel[~df_rel["author"].isin(authors_common)]["followers"]
    z = df_unr[~df_unr["author"].isin(authors_common)]["followers"]
    sb.histplot(x+1e-5, log_scale=True, stat="probability", label="Reliable", color="#2f4b7c")
    sb.histplot(z+1e-5, log_scale=True, stat="probability", label="Unreliable", color="#f95d6a")
    plt.savefig("../results/cited-followers-exclusive.pdf", format="pdf")
    plt.clf()

if plot_age_distribution:
    x_rel = np.array([get_account_age(user_data[u]) for u in df_rel["author"].unique() if u in user_data], dtype=int)
    x_unr = np.array([get_account_age(user_data[u]) for u in df_unr["author"].unique() if u in user_data], dtype=int)

    sb.histplot(x_rel, log_scale=False, stat="probability", label="Reliable", color="#2f4b7c")
    sb.histplot(x_unr, log_scale=False, stat="probability", label="Unreliable", color="#f95d6a")
    plt.legend()
    plt.savefig("../results/age-distribution.pdf", format="pdf")
    plt.clf()


# Reset filter
# Filter dataframes, only include followers > 0
df_rel = df[(df["label"] == 0) & (df["source"].isin(sources_in))]
df_unr = df[(df["label"] == 1) & (df["source"].isin(sources_in))]


# df_rel = df_rel.drop_duplicates(["source", "author"])
# df_unr = df_unr.drop_duplicates(["source", "author"])
# df = df.drop_duplicates(["source", "author"])

for d, name in zip([df_rel, df_unr, df], ["rel", "unr", "all"]):

    print("=====", name)
    # d = d[~d["author"].isin(exclude_authors)]
    # Compute the correlation between no. of followers and prob. of being cited in the news.
    num_cited = dict(d["author"].value_counts())
    num_followers = d.drop_duplicates(["author"])[["author", "followers"]].set_index("author").to_dict("index")

    sus = len(d[d["followers"] == -1].drop_duplicates(["author"]))
    verified = sum([1 if user_data[u]["verified"] is True else 0 for u in d["author"].unique()
                    if u in user_data and num_cited[u] > 0])
    avg_age = np.mean([get_account_age(user_data[u]) for u in d["author"].unique() if u in user_data])
    print(" == Total cited", len(d))
    print(" == Suspended", sus, sus/len(d))
    print(" == Verified", verified, verified/len(d))
    print(" == Avg. age (days)", avg_age)

    print(" -- Most cited accounts")
    k = 10
    for usr in sorted(num_cited, key=lambda u: num_cited[u], reverse=True)[:k]:
        # print(usr, "%.4f" % (num_cited[usr]/sum(num_cited.values())), num_cited[usr], num_followers[usr]["followers"],
        #       end="\\\\\n",
        #       sep=" & ")
        ver = "Yes" if usr in user_data and user_data[usr]["verified"] is True else "No"
        print(usr, num_cited[usr], num_followers[usr]["followers"], ver, get_account_age(user_data[usr])
              if usr in user_data else None,
              end=" \\\\\n",
              sep=" & ")
    print("---------")

    # print("  -- Low profile accounts")
    # k = 10
    # for usr in sorted([u for u in num_cited if -1 < num_followers[u]["followers"] < 200],
    #                   key=lambda u: num_cited[u],
    #                   reverse=True)[:k]:
    #     print(usr, "%.4f" % (num_cited[usr]/sum(num_cited.values())), num_cited[usr], num_followers[usr])
    # print("---------")

    x_cited = list()
    x_followers = list()
    for author in sorted(num_cited):
        if num_followers[author]["followers"] > 0 and num_cited[author] > 0:
            x_followers.append(num_followers[author]["followers"])
            x_cited.append(num_cited[author])

    x_cited = np.array(x_cited, dtype=np.int32)
    x_followers = np.array(x_followers, dtype=np.int32)
    plt.scatter(x_cited, x_followers)
    plt.yscale("log")
    plt.xscale("log")
    plt.xlabel("Citations")
    plt.ylabel("Followers")
    plt.savefig("../results/followers-cited-correlation-%s.png" % name, format="png")
    plt.clf()

    rho = spearmanr(x_cited, x_followers)
    r = pearsonr(x_cited, x_followers)
    print("Spearman rho:", rho)
    print("Pearson r:", r)


