import json
import os

path = "missing_accounts"
path_ud = "user_data/user_list.json"

with open(path_ud) as fin:
    user_list = json.load(fin)

data = list()
for root, dirs, files in os.walk(path):
    for f in files:
        with open(os.path.join(root, f)) as fin:
            data.extend(json.load(fin))

print(len(data))
for d in data:
    if d["username"].lower() == "realdonaldtrump":
        print(d["username"])

user_list.extend(data)

user_data = dict()

for user in user_list:
    user_data[user["username"]] = user

print(len(user_data), "users.")
with open("user_data/user_data.json", "w") as fout:
    json.dump(user_data, fout)
