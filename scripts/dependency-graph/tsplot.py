import os, sys
import metadata
import pandas as pd
import numpy as np
import random
import math
from collections import defaultdict
from statsmodels.tsa.stattools import adfuller

from plot import plt, sns

def draw(path, srv):
     filename = os.path.join(path, srv["preprocessed_filename"])
     df = pd.read_csv(filename, sep="\t", index_col='time', parse_dates=True)
     bins = defaultdict(list)
     for i, col in enumerate(df.columns):
         serie = df[col].dropna()
         if pd.algos.is_monotonic_float64(serie.values, False)[0]:
             serie = serie.diff()[1:]
         p_value = adfuller(serie, autolag='AIC')[1]
         if math.isnan(p_value): continue
         nearest = 0.05 * round(p_value/0.05)
         bins[nearest].append(serie)
     for bin, members in bins.items():
         series = [serie.name for serie in members]
         if len(members) <= 10:
             columns = series
         else:
             columns = random.sample(series, 10)

         subset = df[columns]
         name = "%s_adf_confidence_%.2f.png" % (srv["name"], bin)
         print(name)
         axes = subset.plot(subplots=True)
         plt.savefig(os.path.join(path, name))
         plt.close("all")

if __name__ == '__main__':
    if len(sys.argv) < 1:
        sys.stderr.write("USAGE: %s measurment\n" % sys.argv[0])
        sys.exit(1)
    for path in sys.argv[1:]:
        services = metadata.load(path)["services"]
        for srv in services:
            draw(path, srv)
