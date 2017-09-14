import sys
import pandas as pd
import numpy as np
from plot import plt, sns

def build_graph(image, paths):
    fig, axes = plt.subplots(nrows=4, figsize=(10,40))
    for i, col in enumerate(["p_for_lag_1", "p_for_lag_2", "p_for_lag_3", "p_for_lag_4"]):
        p_values = []
        for path in paths:
            #df = pd.read_csv(path, sep="\t", dtype=dict(p_for_lag_1=np.float64))
            df = pd.read_csv(path, sep="\t", compression='gzip')
            # bug in causation!
            df.drop_duplicates(subset=["perpetrator", "consequence"], inplace=True)
            rows = df.iterrows()
            while True:
                try:
                    row_a = next(rows)[1]
                    row_b = next(rows)[1]
                except StopIteration:
                    break
                if row_a[col] < row_b[col]:
                    p_value = row_a[col]
                else:
                    p_value = row_b[col]
                p_values.append(p_value)
        ax = axes[i]
        ax.hist(p_values, bins=20, stacked=True, normed=True)
        ax.set_title(col)
        ax.set_ylabel("#elements")
        ax.set_xlabel("p value [%]")
        plt.savefig(image, dpi=200)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s image.png causality_files...\n" % sys.argv[0])
        sys.exit(1)
    build_graph(sys.argv[1], sys.argv[2:])
