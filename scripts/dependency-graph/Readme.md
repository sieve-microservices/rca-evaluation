## Scripts to analyze Timeseries

python deps for clustering:

```
$ sudo apt install liblapack-dev libgfortran-4.8-dev libatlas-base-dev libatlas-dev python-dev python-virtualenv pkg-config libfreetype6-dev gfortran
# for interactive graphs
$ sudo apt-get build-dep python-matplotlib
# setup direnv and then do:
$ pip install -r requirements.txt
```

# Process after measuring stuff

1. preprocess: `$ python preprocess.py <measurement>`
2. kshape cluster + graphs: `$ python cluster.py <measurement>`

# Granger Causility

1. `python causality.py <measurement>`
2. `python depedencegraph <measurement>/*-causility.tsv`

# Redis Queue for distributed computing

1. Ensure measurment directory is shared via nfs across the cluster
2. `python worker.py` on each worker
3. `python submit_jobs.py <measurement> #see submit_jobs.py for details`
