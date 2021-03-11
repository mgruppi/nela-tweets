import json
import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd
import numpy as np


def main():
    path = "../user_data/user_data.json"

    with open(path) as fin:
        data = json.load(fin)

    df = pd.json_normalize(data)

    print(len(df), "users.")
    print("Columns:", df.columns)

    x = df["public_metrics.following_count"].sample(n=10000).astype(np.int32)
    sb.histplot(1e-0 + x, log_scale=True, stat="density")
    plt.show()


if __name__ == "__main__":
    main()
