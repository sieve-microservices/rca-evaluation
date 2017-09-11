import os
import sys
from collections import defaultdict
import pandas as pd
import numpy as np
import metadata

def load_timeseries(filename, service):
    df = pd.read_csv(filename, sep="\t", index_col='time', parse_dates=True)
    if "cpu" in df and not df["cpu"].isnull().all():
        df = df[df["cpu"].isnull() | (df["cpu"] == "cpu-total")]
    return df[service["fields"]]

def is_monotonic(serie):
    if serie.dtype == np.float64:
        return pd.algos.is_monotonic_float64(serie.values, False)[0]
    elif serie.dtype == np.int64:
        return pd.algos.is_monotonic_int64(serie.values, False)[0]
    else:
        raise ValueError("unexpected column type: %s" % serie.dtype)

def classify_series(df):
    classes = defaultdict(list)
    for c in df.columns:
        if df[c].isnull().any():
            key = "empty_fields"
        elif df[c].var() == 0:
            key = "constant_fields"
        elif is_monotonic(df[c]):
            key = "monotonic_fields"
        else:
            key = "other_fields"
        classes[key].append(c)
    return classes

def interpolate_missing(df):
    # might improve accuracy
    # df = df.interpolate(method="time", limit_direction="both")
    # df.fillna(method="bfill", inplace=True)
    df = df.resample("500ms").mean()
    df.interpolate(method="time", limit_direction="both", inplace=True)
    df.fillna(method="bfill", inplace=True)
    return df

def apply(path):
    data = metadata.load(path)
    for service in data["services"]:
        filename = os.path.join(path, service["filename"])
        df = load_timeseries(filename, service)
        print(service)
        df2 = interpolate_missing(df[service["fields"]])
        classes = classify_series(df2)
        preprocessed_series = {}
        for k in classes["other_fields"]:
            # short by one value, because we have to short the other one!
            preprocessed_series[k] = df2[k][1:]
        for k in classes["monotonic_fields"]:
            preprocessed_series[k + "-diff"] = df2[k].diff()[1:]
        newname = service["name"] + "-preprocessed.tsv.gz"
        df3 = pd.DataFrame(preprocessed_series)
        df3.to_csv(os.path.join(path, newname), sep="\t", compression='gzip')
        service["preprocessed_filename"] = newname
        service["preprocessed_fields"] = list(df3.columns)
        service.update(classes)
    metadata.save(path, data)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurements\n" % sys.argv[0])
        sys.exit(1)

    for p in sys.argv[1:]:
        apply(p)
