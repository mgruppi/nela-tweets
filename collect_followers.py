from twitter_api import TwitterAPI
import json
import os
import argparse


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Path to input list of users.")
    parser.add_argument("--only_following", action="store_true", help="Only collect following (not followers).")

    args = parser.parse_args()
    path_user = args.path

    with open("oauth2.key") as fin:
        auth = json.load(fin)
    api = TwitterAPI(auth)

    with open(path_user) as fin:
        user_data = json.load(fin)

    out_path = "follows"
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    for user in user_data:
        _id = user["id"]

        if not args.only_following:  # Skip followers
            followers = api.get_all_follows(_id, endpoint="followers")
            with open(os.path.join(out_path, "%s-followers.json" % _id), "w") as fout:
                json.dump(followers, fout)

        following = api.get_all_follows(_id, endpoint="following")
        with open(os.path.join(out_path, "%s-following.json" % _id), "w") as fout:
            json.dump(following, fout)


if __name__ == "__main__":
    main()
