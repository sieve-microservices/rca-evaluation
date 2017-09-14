import os, sys
import metadata
from plot import plt, sns
import pandas as pd
import numpy as np
import scipy.stats as stats
from statsmodels.tsa.stattools import adfuller
from preprocess import load_timeseries, is_monotonic
import math

def do_adfuller(path, srv, p_values):
    filename = os.path.join(path, srv["filename"])
    df = load_timeseries(filename, srv)
    columns = []
    for c in df.columns:
        if (not df[c].isnull().all()) and df[c].var() != 0:
            columns.append(c)
    df = df[columns]
    if len(columns) == 0: return []
    for i, col in enumerate(df.columns):
        serie = df[col].dropna()
        if is_monotonic(serie):
            serie = serie.diff()[1:]

        for reg in p_values:
            v = adfuller(serie, regression=reg)[1]
            if math.isnan(v): # uncertain
                p_values[reg].append(-0.1)
            else:
                p_values[reg].append(v)

    return p_values

def draw(path):
    data = metadata.load(path)
    adf_dist_path = os.path.join(path, "adf_distribution.png")
    if os.path.exists(adf_dist_path):
        print("path exists %s, skip" % adf_dist_path)
        #return
    p_values = {'c': [], 'ct': [], 'ctt': []}
    for srv in data["services"]:
        do_adfuller(path, srv, p_values)

    measurement = os.path.dirname(os.path.join(path,''))
    ax = plt.subplots(1)[1]
    ax.yaxis.grid()
    labels = ["constant", "constant + trend", "constant, and linear and quadratic trend"]
    ax.hist(p_values.values(), 22, histtype='bar', align='mid', label=labels, alpha=0.4)
    ax.set_xlabel("Distribution of p-value for Augmented Dickey-Fuller test for %s" % measurement)
    ax.legend()
    plt.savefig(adf_dist_path)
    print(adf_dist_path)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurement..." % sys.argv[0])
        sys.exit(1)
    for p in sys.argv[1:]:
        plt.clf()
        draw(p)
