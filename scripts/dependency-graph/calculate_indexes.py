from sklearn.metrics import silhouette_score
from kshape import kshape, _sbd
from collections import defaultdict

import numpy as np
import pandas as pd
import metadata
import sys, os

def gap(centroids, data, labels, refs=None, nrefs=20, ks=range(1,11)):
    """
    Compute the Gap statistic for an nxm dataset in data.
    Either give a precomputed set of reference distributions in refs as an (n,m,k) scipy array,
    or state the number k of reference distributions in nrefs for automatic generation with a
    uniformed distribution within the bounding box of data.
    Give the list of k-values for which you want to compute the statistic in ks.
    """
    shape = data.shape
    if refs==None:
      tops = data.max(axis=0)
      bots = data.min(axis=0)
      dists = np.matrix(np.diag(tops - bots))

      rands = np.random.random_sample(size=(shape[0], shape[1], nrefs))
      for i in range(nrefs):
          rands[:,:,i] = rands[:,:,i] * dists + bots
    else:
        rands = refs

    gaps = np.zeros((len(ks),))
    for (i,k) in enumerate(ks):
        disp = sum(_sbd([data[m,:], centroids[labels[m],:]) for m in range(shape[0])])

        refdisps = np.zeros((rands.shape[2],))
        for j in range(rands.shape[2]):
            for centroid, indicies in kshape(rands[:, :, j], k):
                for index in indicies:
                    refdisps[j] += _sbd(rands[index, :, j], centroid)[0]
        gaps[i] = np.log(np.mean(refdisps))- np.log(disp)
    return gaps

def process_service(path, service_name, res):
  scores = []
  for i in range(2, 8):
      centroids = []
      metrics = []
      labels = []
      for j in range(i):
          name = "%s-cluster-%d_%d.tsv" % (service_name, i, j + 1)
          cluster_path = os.path.join(path, name)
          df = pd.read_csv(cluster_path, sep="\t", index_col='time', parse_dates=True)
          centroids.append(df.columns[0])
          for idx, c in enumerate(df.columns[1:]):
              metrics.append(df[c])
              labels.append(j)
      distances = np.zeros([len(metrics), len(metrics)])
      for idx_a, metric_a in enumerate(metrics):
          for idx_b, metric_b in enumerate(metrics):
              distances[idx_a, idx_b] = _sbd(metric_a, metric_b)[0]
      labels = np.array(labels)
      # def gap(centroids, data, labels, refs=None, nrefs=20, ks=range(1,11)):
      score = gap(centroids, np.array(metrics), labels)
      #if len(np.unique(labels)) == 1:
      #    score = -1
      #else:
      #    score = silhouette_score(distances, labels, metric='precomputed')
      res["name"].append(service_name)
      res["cluster"].append(i)
      res["silhouette_score"].append(score)
      res["best_cluster"].append(None)
      scores.append(score)
  best = np.argmax(scores)
  res["name"].append(service_name)
  res["cluster"].append("best")
  res["silhouette_score"].append(scores[best])
  res["best_cluster"].append(best + 2)

def main(path):
    data = metadata.load(path)
    result = defaultdict(list)
    for srv in data["services"]:
        process_service(path, srv["name"], result)
    n = os.path.join(path, "scores.tsv")
    print(n)
    pd.DataFrame(result).to_csv(n)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurement" % sys.argv[0])
        sys.exit(1)
    main(sys.argv[1])
