import os
import sys
import json
import argparse
import multiprocessing as mp 
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from sklearn.metrics import silhouette_score as _silhouette_score
from metricsnamecluster import cluster_words
import metrics_utils as msu

import graphs
from kshape import kshape, zscore, _sbd
import metadata

from collections import defaultdict

metadata_lock = mp.Lock()

def silhouette_score(series, clusters):
    distances = np.zeros((series.shape[0], series.shape[0]))
    for idx_a, metric_a in enumerate(series):
        for idx_b, metric_b in enumerate(series):
            distances[idx_a, idx_b] = _sbd(metric_a, metric_b)[0]
    labels = np.zeros(series.shape[0])
    for i, (cluster, indicies) in enumerate(clusters):
        for index in indicies:
            labels[index] = i

    # silhouette is only defined, if we have 2 clusters with assignments at 
    # minimum
    if len(np.unique(labels)) == 1 or (len(np.unique(labels)) >= distances.shape[0]):
    #if len(np.unique(labels)) == 1:
        return labels, -1
    else:
        return labels, _silhouette_score(distances, labels, metric='precomputed')

def do_kshape(name_prefix, df, cluster_size, initial_clustering=None):
    columns = df.columns
    matrix = []
    for c in columns:
        matrix.append(zscore(df[c]))
    res = kshape(matrix, cluster_size, initial_clustering)
    labels, score = silhouette_score(np.array(matrix), res)

    # keep a reference of which metrics are in each cluster
    cluster_metrics = defaultdict(list)
    # we keep it in a dict: cluster_metrics[<cluster_nr>]{<metric_a>, <metric_b>}
    for i, col in enumerate(columns):
        cluster_metrics[int(labels[i])].append(col)

    filenames = []
    for i, (centroid, assigned_series) in enumerate(res):
        d = {}
        for serie in assigned_series:
            d[columns[serie]] = pd.Series(matrix[serie], index=df.index)
        d["centroid"] = pd.Series(centroid, index=df.index)
        df2 = pd.DataFrame(d)
        figure = df2.plot()
        figure.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        name = "%s_%d" % (name_prefix, (i+1))
        filename = name + ".tsv.gz"
        print(filename)
        df2.to_csv(filename, sep="\t", compression='gzip')
        filenames.append(os.path.basename(filename))
        graphs.write(df2, name + ".png")
    return cluster_metrics, score, filenames

def get_initial_clustering(service, metadata, metrics):

    s_score, cluster_metrics = msu.get_cluster_metrics(metadata, service)

    if not cluster_metrics:
        return None

    common_metrics_all = set()
    initial_idx = np.zeros(len(metrics), dtype=np.int)

    for key, value in cluster_metrics.iteritems():
        other_metrics = value['other_metrics']

        common_metrics = set(metrics) & set(other_metrics)
        for metric in list(common_metrics):
            initial_idx[list(metrics).index(metric)] = int(key)

        common_metrics_all = (common_metrics_all | common_metrics)

    # assign remaining metrics to a new cluster (if any)
    remaining_metrics = list(metrics - common_metrics_all)
    remaining_cluster = max(initial_idx) + 1
    if remaining_metrics:
        for metric in remaining_metrics:
            initial_idx[list(metrics).index(metric)] = remaining_cluster

    return initial_idx

def cluster_service(path, service, cluster_size, prev_metadata=None):

    filename = os.path.join(path, service["preprocessed_filename"])
    df = pd.read_csv(filename, sep="\t", index_col='time', parse_dates=True)

    initial_idx = None
    if prev_metadata:
        initial_idx = get_initial_clustering(service["name"], prev_metadata, df.columns)
        # adjust cluster_size if an initial assigment has been found
        if initial_idx is not None:
            cluster_size = len(np.unique(initial_idx))

    prefix = "%s/%s-cluster-%d" % (path, service["name"], cluster_size)
    if os.path.exists(prefix + "_1.png"):
        print("skip " + prefix)
        return (None, None)

    cluster_metrics, score, filenames = do_kshape(prefix, df, cluster_size, initial_idx)
    if cluster_size < 2:
        # no silhouette_score for cluster size 1
        return (None, None)
    print("silhouette_score: %f" % score)

    # protect the write access to the metadata file
    metadata_lock.acquire()
    with metadata.update(path) as data:
        for srv in data["services"]:
            if srv["name"] == service["name"]:
                if "clusters" not in srv:
                    srv["clusters"] = {}
                d = dict(silhouette_score=score, filenames=filenames, metrics=cluster_metrics)
                srv["clusters"][cluster_size] = d
    metadata_lock.release()

    return (service["name"], cluster_size)

if __name__ == '__main__':

    # use an ArgumentParser for a nice CLI
    parser = argparse.ArgumentParser()

    # options (self-explanatory)
    parser.add_argument(
        "--msr-dir", 
         help = """dir w/ preprocessed data.""")

    parser.add_argument(
        "--initial-cluster-dir", 
         help = """dir w/ clustered data from which to derive initial cluster 
                   assigments.""")

    args = parser.parse_args()

    # quit if a dir w/ causality files hasn't been provided
    if not args.msr_dir:
        sys.stderr.write("""%s: [ERROR] please supply 1 measurement data dir\n""" % sys.argv[0]) 
        parser.print_help()
        sys.exit(1)

    if args.initial_cluster_dir:
        prev_metadata = metadata.load(args.initial_cluster_dir)
    else:
        prev_metadata = None

    last_cluster_size = defaultdict(int)

    start_time = datetime.utcnow()
    for n in range(2, 7):

        # to reduce clustering time, use paralellism
        pool = mp.Pool(mp.cpu_count())

        # tasks to run in paralell
        tasks = []
        for srv in metadata.load(args.msr_dir)["services"]:

            if last_cluster_size[srv["name"]] > n:
                continue

            tasks.append((args.msr_dir, srv, n, prev_metadata))

        if len(tasks) > 0:
            jobs_remaining = len(tasks)
            results = [pool.apply_async(cluster_service, t) for t in tasks]

            for result in results:
                jobs_remaining = jobs_remaining - 1
                (service, cluster_size) = result.get()
                if service is not None:
                    last_cluster_size[service] = cluster_size
                    print("finished pair (%s,%d). %d jobs remaining." 
                        % (service, cluster_size, jobs_remaining))

        # keep things tidy
        pool.close()
        pool.join()

        print results

    end_time = datetime.utcnow()
    sys.stdout.write(
        "%s : [INFO]: clustering finished. started @ %s, ended @ %s.\n" 
        % (sys.argv[0], str(start_time), str(end_time)))

