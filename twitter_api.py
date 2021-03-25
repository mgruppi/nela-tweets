import sqlite3
from network import get_tweet_authors, load_all_tweets
import json
import requests
from urllib.parse import urljoin
import time
import os
from datetime import datetime
import argparse


class TwitterAPI:
    def __init__(self, credentials):
        self.credentials = credentials
        self.root_url = "https://api.twitter.com/2/"
        self.session = requests.Session()
        self.headers = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer %s" % credentials["bearer_token"]}

    def get_users(self, usernames):

        fields = ["created_at", "description", "entities", "id", "location", "name",
                  "pinned_tweet_id", "profile_image_url", "protected", "public_metrics",
                  "url", "username", "verified", "withheld"]
        url = urljoin(self.root_url, "users/by")

        payload = {"usernames": ",".join(usernames),
                   "user.fields": ",".join(fields)}

        response = self.session.get(url, headers=self.headers, params=payload)
        if response.status_code == 200:
            data = response.json()
            return response.status_code, data
        else:
            return response.status_code, response

    def get_users_batch(self, usernames, path_out="user_data", return_any=False):
        """
        Given an input list of usernames, send requests through the Twitter API to get user info.
        Send batches of 100 users at a time, once the rate limit is exceeded, sleep for 15 minutes.
        Before sleeping, dumps current batches into a JSON in user_data/.
        :param usernames: List of usernames to query from Twitter.
        :param path_out: Path to save output JSONs.
        :param return_any: Returns user regardless of the HTTP response obtained.
        :return:
        """
        batch_size = 100
        data = list()
        count = 0  # count successful queries
        for i in range(0, len(usernames), 100):
            try:
                print("(%d-%d) : %d" % (i, i+batch_size-1, count))
                batch = usernames[i:i+batch_size]
                if len(batch) == 0:
                    continue
                status_code = 429  # start with too many requests
                while status_code == 429:
                    status_code, r = self.get_users(batch)
                    if status_code == 200:
                        data.extend(r["data"])
                        count += len(r["data"])
                    elif status_code == 429:
                        # We are about to sleep, dump current data and sleep.
                        fname = datetime.now().strftime("%s") + ".json"
                        with open(os.path.join(path_out, fname), "w") as fout:
                            json.dump(data, fout)
                            print("Saved ", len(data), "users.")
                        data = list()
                        # Sleep before next loop
                        time.sleep(60*15)  # Sleep for 15 minutes before next batch.
                    elif return_any:
                        data.extend(r)
            except Exception as e:
                print(e)

        if len(data) > 0:
            fname = datetime.now().strftime("%s") + ".json"
            with open(os.path.join(path_out, fname), "w") as fout:
                json.dump(data, fout)

        return data

    def get_follows(self, user_id, next_token=None, endpoint="followers", max_results=1000):
        """
        Retrieves the list of followers/following users given a user id.
        :param user_id: Twitter ID of the input user.
        :param next_token: Next page token, if needed.
        :param endpoint: One of {"followers", "following"}. Whether to retrieve list of followers of following.
        :param max_results: Maximum number of results to retrieve.
        :return data: JSON with returned data, if successful.
        """
        url = urljoin(self.root_url, "users/%s/%s" % (user_id, endpoint))

        fields = ["created_at", "description", "entities", "id", "location", "name",
                  "pinned_tweet_id", "profile_image_url", "protected", "public_metrics",
                  "url", "username", "verified", "withheld"]

        payload = {"pagination_token": next_token, "max_results": max_results,
                   "user.fields": ",".join(fields)}
        response = self.session.get(url, headers=self.headers, params=payload)

        if response.status_code == 200:
            data = response.json()
            return response.status_code, data
        else:
            return response.status_code, None

    def get_all_follows(self, user_id, endpoint="followers"):
        data = list()
        next_token = None
        while True:
            try:
                status, r = self.get_follows(user_id, endpoint=endpoint, next_token=next_token)
                if status == 200:
                    if "data" not in r:
                        break
                    data.extend(r["data"])
                    print("%d" % len(data))
                    if "next_token" in r["meta"]:
                        next_token = r["meta"]["next_token"]
                    else:
                        break
                elif status == 429:
                    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "- Sleeping...")
                    time.sleep(60*15)  # sleep for 15 minutes
                else:
                    break
            except Exception as e:
                print(status, e)
        return data

    def get_follows_batch(self, usernames, path_out=["follows_data"]):
        """

        :param usernames:
        :param path_out:
        :return:
        """
        batch_size = 100
        data = list()
        count = 0
        for i in range(0, len(usernames), 100):
            try:
                print("(%d-%d) : %d" % (i, i+batch_size-1, count))
                batch = usernames[i:i+batch_size]
                if len(batch) == 0:
                    continue
                status_code = 429

                while status_code == 429:
                    status_code, r = self.get_follows(batch)
                    if status_code == 200:
                        data.extend(r["data"])
                        count += len(r["data"])
                    elif status_code == 429:
                        fname = datetime.now().strftime("%s") + ".json"
                        with open(os.path.join(path_out, fname), "w") as fout:
                            json.dump(data, fout)
                            print("Saved ", len(data), "users.")
                        data = list()
                        # Sleep before next loop
                        time.sleep(60*15)  # Sleep for 15 minutes before next batch.

            except Exception as e:
                print(e)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=str, help="Path to NELA database.")

    args = parser.parse_args()
    path = args.database

    with open("oauth2.key") as fin:
        credentials = json.load(fin)

    path_user = "user_data"
    if not os.path.exists(path_user):
        os.mkdir(path_user)

    api = TwitterAPI(credentials)
    con = sqlite3.connect(path)
    tweets = load_all_tweets(con)
    t_ids = [t[2] for t in tweets]
    authors = get_tweet_authors(t_ids)

    print(len(authors), "authors")
    user_list = list(authors)
    data = api.get_users_batch(user_list)


if __name__ == "__main__":
    main()
