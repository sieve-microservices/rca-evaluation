import sys
import os

from jinja2 import Environment

import metadata
import pandas as pd
from collections import defaultdict

INDEX = """
# Measurement {{title}}

## Overview
| Name | #Metrics | #Metrics exclu. constant | Cluster sizes | Silhouette score |
|------|----------|--------------------------|---------------|------------------|
{% for _, runs in services -%}
{% for _, r in runs.iterrows() -%}
| {{r["name"]}} | {{r["fields"]|length}} | {{r["preprocessed_fields"]|length}} | {{cluster_links(r, r.title)}} | {{silhouette_score(r, r.title)}} |
{% endfor -%}
{% endfor %}
"""

CLUSTER = """
# Measurement {{title}} with a cluster size: {{cluster_size}}

{% for name,number,url in clusters -%}
## Cluster {{number}}
- ![{{name}}]({{url}})
{% endfor -%}
"""

def cluster_number(service):
    if "clusters" in service:
        return max(map(int, service["clusters"].keys()))
    else:
        import pdb; pdb.set_trace()
        return 0

def cluster_links(service, title):
    links = []
    for i in range(1, cluster_number(service) + 1):
        links.append("[%d](%s-%s-%d)" % (i, title, service["name"], i))
    return " ".join(links)

def silhouette_score(row, title):
    res = []
    best = -1
    best_score = -1
    for key, value in sorted(row.clusters.items(), key=lambda v: int(v[0])):
        score = value.get("silhouette_score", -1)
        res.append("%s=%.2f" % (key, score))
        if best_score < score:
            best = key
            best_score = score
    if best == -1:
        res.append("-> no cluster found")
    else:
        res.append("-> best [%s](%s-%s-%s)" % (best, title, row["name"], best))
    return " ".join(res)

def write_template(template, path, **kwargs):
    template = Environment().from_string(template)
    content = template.render(**kwargs)
    print(path)
    with open(path, "w+") as f:
        f.write(content)

def write_measurement(measurement, report):
    if measurement.endswith("/"):
        measurement = measurement[:-1]
    title = os.path.basename(measurement)
    data = metadata.load(measurement)
    metrics_count = 0
    metrics_set = set()
    filtered_count = 0
    for srv in data["services"]:
        metrics_count += len(srv["fields"])
        metrics_set.update(srv["fields"])
        filtered_count += len(srv["preprocessed_fields"])
        for i in range(1, cluster_number(srv) + 1):
            clusters = []
            for j in range(1, i+1):
                name = "%s-cluster-%d_%d.png" % (srv["name"], i, j)
                url = "https://gitlab.com/micro-analytics/measurements2/raw/master/%s/%s" % (title, name)
                clusters.append((name, j, url))
            args = dict(title=title, cluster_size=i, clusters=clusters)
            path = os.path.join(report, "%s-%s-%d.md" % (title, srv["name"], i))
            write_template(CLUSTER, path, **args)
    return title, data["services"]

def write(index_title, report, measurements):
    m = defaultdict(list)
    for measurement in measurements:
        title, services = write_measurement(measurement, report)
        for srv in services:
            for k,v in srv.items():
                m[k].append(v)
            assert "title" not in srv
            m["title"].append(title)
    df = pd.DataFrame(m).groupby("name")
    df["clusters"].fillna({})
    args = dict(silhouette_score=silhouette_score, cluster_links=cluster_links, services=df, title=title)
    index = os.path.join(report, index_title + ".md")
    write_template(INDEX, index, **args)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.stderr.write("USAGE: %s title report measurements...\n" % sys.argv[0])
        sys.exit(1)
    title = sys.argv[1]
    report = sys.argv[2]
    measurements = sys.argv[3:]
    write(title, report, measurements)
