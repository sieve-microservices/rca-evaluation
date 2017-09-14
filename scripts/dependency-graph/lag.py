import os
import sys
import json

import pandas as pd
from plot import plt, sns

from kshape import kshape, zscore

def load_metadata(path):
    with open(os.path.join(path, "metadata.json")) as f:
        return json.load(f)

def centroids(path):
    metadata = load_metadata(path)
    d = {}
    for srv in metadata["services"]:
        name = "%s/%s-cluster-1_1.tsv" % (path, srv["name"])
        df = pd.read_csv(name, sep="\t", index_col='time', parse_dates=True)
        d[srv["name"]] = df.centroid
    df2 = pd.DataFrame(d)
    df2 = df2.fillna(method="bfill", limit=1e9)
    df2 = df2.fillna(method="ffill", limit=1e9)
    fig = df2.plot()
    handles, labels = fig.get_legend_handles_labels()
    fig.grid('on')
    lgd = fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5,-0.1))
    plt.savefig("graph.png", bbox_extra_artists=(lgd,), bbox_inches='tight')
    plt.close("all")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurement\n" % argv[0])
        sys.exit(1)
    centroids(sys.argv[1])
