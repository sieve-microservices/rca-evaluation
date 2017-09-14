# 'system' modules
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
import json
import time

# custom modules
import metadata
import common
import rca_plots
import metrics_utils as mu

from collections import defaultdict
from collections import OrderedDict
from itertools import izip_longest
from rca_utils import Graph
from prettytable import PrettyTable

P_SCORE_ON = False
GC_SIGNIFICANCE_DEFAULT = 0.001
APP_METRIC_DELIMITER = "|"
SELECT_BY_EXCLUSION = True

INCLUDE=["nova", "neutron", "glance", "keystone", "horizon", "rabbitmq"]
EXCLUDE=[
    "swift_proxy_server",
    "swift_object_updater",
    "swift_object_replicator",
    "swift_object_auditor",
    "swift_object_server",
    "swift_container_updater",
    "swift_container_replicator",
    "swift_container_auditor",
    "swift_container_server",
    "swift_account_reaper",
    "swift_account_replicator",
    "swift_account_auditor",
    "swift_account_server",
    "swift_rsyncd"]

GC_LAGS = [
    "p_for_lag_1",
    "p_for_lag_2",
    "p_for_lag_3",
    "p_for_lag_4",
    "p_for_lag_5"
]

GRAPH_A = [
    "a.m11 < b.m13|1.0",
    "a.m10 > b.m12|1.0",
    "a.m1 > b.m2|1.0",
    "a.m1 < b.m2|1.0",
    "a.m4 > c.m1|1.0",
    "a.m5 > c.m2|1.0",
    "a.m6 < d.m3|1.0,2.0",
    "a.m9 < d.m7|1.0",
    "a.m3 < d.m8|1.0,2.0",
    "a.m3 <> d.m10|1.0"
]

GRAPH_B = [
    "a.m15 < b.m16|1.0",
    "a.m13 > b.m14|1.0",
    "a.m1 > b.m2|2.0",
    "a.m1 < b.m2|1.0,2.0",
    "a.m1 < b.m5|1.0",
    "a.m4 > e.m1|1.0",
    "a.m5 > e.m2|1.0",
    "a.m6 < d.m3|1.0",
    "a.m9 < d.m7|1.0"
]

def build_dummy_graph(glines, graph):

    for gline in glines:

        edge_str = gline.split("|", 1)[0]
        lags_str = gline.split("|", 1)[1]
        lags = [float(l) for l in lags_str.split(",")]

        if " > " in edge_str:
            delim = " > "
            p_index = 0
            c_index = 1
            is_bidir = False
        elif " < " in edge_str:
            delim = " < "
            p_index = 1
            c_index = 0
            is_bidir = False
        elif " <> " in edge_str:
            delim = " <> "
            p_index = 0
            c_index = 1
            is_bidir = True

        perp = edge_str.split(delim, 1)[p_index]
        p_metric = perp.split(".", 1)[1]
        perp = perp.split(".", 1)[0]

        conseq = edge_str.split(delim, 1)[c_index]
        c_metric = conseq.split(".", 1)[1]
        conseq = conseq.split(".", 1)[0]

        graph.add_edge(perp, p_metric, 1.0, conseq, c_metric, lags, is_bidir)

def run_demo(updated_services):

    # build both graphs from the edge descriptions
    graph_a = Graph()
    build_dummy_graph(GRAPH_A, graph_a)

    graph_b = Graph()
    build_dummy_graph(GRAPH_B, graph_b)

    # print them as .dot files for reference
    graph_a.print_graph("graph-a", True, True)
    graph_b.print_graph("graph-b", False, True)

    # generate the differences by considering an update on service 'a'
    start_time = time.time()
    diff_graph = graph_a.get_diff(graph_b, updated_services)
    print("--- graph differences calculated in %s seconds --- " % (time.time() - start_time))
    diff_graph.print_graph("graph-diff", False, True)

def get_callgraph_scores():
    return None

def get_metric_scores():
    return None

def run_rca(
    causality_graphs, 
    updated_services = "all",
    filter_edges = None,
    excluded_metrics = []):

    # extract metrics & clusters from metadata
    start_time = time.time()

    for version in causality_graphs:
        causality_graphs[version]['graph'].extract_metadata(metadata.load(causality_graphs[version]['dir']), add_services = ['nova_novncproxy'])
        # causality_graphs[version].print_graph(version, True, False)
        print("%s::run_rca() : extracted \"%s\" metrics & clusters in %s seconds" % (sys.argv[0], version, time.time() - start_time))

    # 1st phase of rca is individual metric differences in metadata
    metric_diffs_by_service, totals = causality_graphs['faulty']['graph'].get_metric_diffs(causality_graphs['non-faulty']['graph'])

    # order the metric_diffs_by_service, according to 'total-change'
    metric_diffs_list = sorted(
        list(metric_diffs_by_service), 
        key = lambda x : (len(metric_diffs_by_service[x]['new']) + len(metric_diffs_by_service[x]['discarded'])), reverse = True)

    # # print a LaTEX formatted table, for paper purposes
    # rca_plots.to_latex_table(metric_diffs_by_service, metric_diffs_list, ['new', 'discarded', 'unchanged'])

    print("\n#1 : individual metric differences:")
    table = PrettyTable(['service', 'new', 'discarded', 'unchanged'])
    for service_name in metric_diffs_list:

        # print("%s [NEW] -> %s" % (service_name, metric_diffs_by_service[service_name]['new']))
        # print("%s [DISCARDED] -> %s" % (service_name, metric_diffs_by_service[service_name]['discarded']))
        # print("%s [UNCHANGED] -> %s" % (service_name, metric_diffs_by_service[service_name]['unchanged']))

        table.add_row([service_name, 
                                    len(metric_diffs_by_service[service_name]['new']), 
                                    len(metric_diffs_by_service[service_name]['discarded']),
                                    len(metric_diffs_by_service[service_name]['unchanged'])])
    table.add_row(["TOTALS",
                    totals['new'],
                    totals['discarded'],
                    totals['unchanged']])
    print(table)
    print("")

    # # plot indiv. metrics
    # rca_plots.plot_individual_metrics(metric_diffs_by_service)

    # 2nd phase : cluster differences

    # calculate cluster difference stats
    cluster_diffs = causality_graphs['faulty']['graph'].get_cluster_diffs(causality_graphs['non-faulty']['graph'])

    print("\n#2.1 : silhouette scores by service:")
    # print silhouette scores by service
    table = PrettyTable(['service', 'silhouette score non-faulty', 'silhouette score faulty'])
    for service_name in cluster_diffs:
        table.add_row([service_name, 
                        cluster_diffs[service_name]['silhouette-score'][1], 
                        cluster_diffs[service_name]['silhouette-score'][0]])

    print(table)
    print("")

    print("\n#2.2 : cluster similarity:")
    # print cluster similarity table
    similarities = []

    table = PrettyTable(['service', 'cluster non-faulty', 'cluster faulty', 'similarity score'])
    for service_name in cluster_diffs:

        is_first = True
        for rep_metric in cluster_diffs[service_name]['similarity']['f-nf']:

            if not is_first:
                name = ""
            else:
                name = service_name
                is_first = False

            similarities.append(cluster_diffs[service_name]['similarity']['f-nf'][rep_metric][1])
            table.add_row([name, 
                            "N/A" if cluster_diffs[service_name]['similarity']['f-nf'][rep_metric][0] is None else cluster_diffs[service_name]['similarity']['f-nf'][rep_metric][0].rep_metric, 
                            rep_metric,
                            cluster_diffs[service_name]['similarity']['f-nf'][rep_metric][1]])

    print(table)
    print("")

    print("\n#2.3 : cluster metric differences")

    # print cluster metric diffs table and gather data for a plot showing the 
    # number of clusters w/ novelty vs. the total number of clusters
    cluster_novelty = []

    # keep total nr. of clusters for 'All' and 'Top' scopes
    # FIXME: this sounds like i'm doing something wrong, but anyway...
    total_changed   = 0
    total_top       = 0
    total_all       = 0

    # the threshold for the top services
    top_threshold = len(metric_diffs_list)
    top_threshold_str = ('Top %d' % (top_threshold))

    table = PrettyTable(['service', 'cluster', 'new', 'discarded', 'unchanged'])
    for service_name in causality_graphs['faulty']['graph'].clusters:

        is_first = True
        for rep_metric in causality_graphs['faulty']['graph'].clusters[service_name]['cluster-table']:

            cluster = causality_graphs['faulty']['graph'].clusters[service_name]['cluster-table'][rep_metric]

            if not is_first:
                name = ""
            else:
                name = service_name
                is_first = False

            columns = {}

            for column_name in ['new', 'discarded', 'unchanged']:
                columns[column_name] = 0 if column_name not in cluster.metric_diffs else len(cluster.metric_diffs[column_name])

            # update the cluster novelty data (for plotting)
            if (columns['new'] > 0) and (columns['discarded'] > 0):
                cluster_novelty.append(('All', 'New\nand\nDiscarded', 1))
                total_changed += 1

                # if service_name in metric_diffs_list[0:top_threshold]:
                #     cluster_novelty.append((top_threshold_str, 'New\nand\nDiscarded', 1))

            elif columns['new'] > 0:
                cluster_novelty.append(('All', 'New', 1))
                total_changed += 1

                # if service_name in metric_diffs_list[0:top_threshold]:
                #     cluster_novelty.append((top_threshold_str, 'New', 1))

            elif columns['discarded'] > 0:
                cluster_novelty.append(('All', 'Discarded', 1))
                total_changed += 1

                # if service_name in metric_diffs_list[0:top_threshold]:
                #     cluster_novelty.append((top_threshold_str, 'Discarded', 1))

            total_all += 1
            if service_name in metric_diffs_list[0:top_threshold]:
                total_top += 1

            table.add_row([name, 
                            cluster.rep_metric,
                            columns['new'],
                            columns['discarded'],
                            columns['unchanged']])

    cluster_novelty.append(('All', 'Changed', total_changed))
    cluster_novelty.append(('All', 'Total', total_all))
    # cluster_novelty.append((top_threshold_str, 'Total', total_top))

    print(table)
    print("")

    print("\n#3.1 : edge differences")

    edge_diffs_stats = []
    cluster_reduction_stats = []

    column_titles = OrderedDict()
    column_titles['new'] = 'New'
    column_titles['discarded'] = 'Discarded'
    column_titles['lag-change'] = 'Lag change'
    # column_titles['changed'] = 'Changed (total)'
    column_titles['unchanged'] = 'Unchanged'

    for similarity_threshold in [0.01, 0.50, 0.60, 0.70]:
        edge_diffs = causality_graphs['faulty']['graph'].get_edge_diffs(
            causality_graphs['non-faulty']['graph'], 
            cluster_diffs, 
            metric_diffs_list[0:top_threshold], 
            similarity_threshold = similarity_threshold)
        
        # print cluster metric diffs table
        table = PrettyTable(['difference-type', 'new', 'discarded', 'unchanged', 'lag-change'])

        total_edge_diffs = 0
        total_metrics = 0
        included_services = set()
        visited_similarity_clusters = set()
        visited_similarity_services = set()
        metrics_per_service = defaultdict(int)

        for edge_diff_type in edge_diffs:

            columns = {}

            for column_name, column_str in column_titles.iteritems():

                if similarity_threshold == 0.01:
                    similarity_threshold = 0.00

                columns[column_name] = 0 if column_name not in edge_diffs[edge_diff_type] else len(edge_diffs[edge_diff_type][column_name])

                # FIXME: ugly form of data collection
                if edge_diff_type == 'similarity': 

                    # if column_name != 'discarded':
                    edge_diffs_stats.append((similarity_threshold, column_str, columns[column_name]))
                    if column_name != 'unchanged':
                        total_edge_diffs += columns[column_name]

                if column_name == 'discarded':
                    graph = causality_graphs['non-faulty']['graph']
                else:
                    graph = causality_graphs['faulty']['graph']

                for edge in edge_diffs[edge_diff_type][column_name]:
                    for cluster in edge:
                        if edge_diff_type == 'similarity' or (edge_diff_type == 'novelty' and similarity_threshold == 0.00):

                            if edge[cluster] not in visited_similarity_clusters:
                                total_metrics += len(graph.clusters[edge[cluster][0]]['cluster-table'][edge[cluster][1]].other_metrics)
                                metrics_per_service[edge[cluster][0]] += len(graph.clusters[edge[cluster][0]]['cluster-table'][edge[cluster][1]].other_metrics)

                            visited_similarity_clusters.add(edge[cluster])
                            visited_similarity_services.add(edge[cluster][0])

                            included_services.add(edge[cluster][0])

            table.add_row([
                edge_diff_type,
                columns['new'],
                columns['discarded'],
                columns['unchanged'],
                columns['lag-change'] ])

        print("included : %s (%d)" % (str(included_services), len(included_services)))

        # edge_diffs_stats.append((similarity_threshold, 'Changed (total)', total_edge_diffs))
        cluster_reduction_stats.append((similarity_threshold, 'Services', len(visited_similarity_services)))
        # print(visited_similarity_clusters)
        cluster_reduction_stats.append((similarity_threshold, 'Clusters', len(visited_similarity_clusters)))
        cluster_reduction_stats.append((similarity_threshold, 'Metrics', total_metrics))

        print("\n\nEDGE DIFFS SUMMARY (SIMILARITY THRESHOLD : %f" % (similarity_threshold))
        print("\tMETRICS (%d) : %s" % (total_metrics, metrics_per_service))
        print("\tEDGES")
        for column_name in edge_diffs['similarity']:

            if column_name == 'discarded':
                graph = causality_graphs['non-faulty']['graph']
            else:
                graph = causality_graphs['faulty']['graph']

            for edge in edge_diffs[edge_diff_type][column_name]:
                print("\t(%s) %s" % (column_name, edge))

    print("")
    print(table)
    print("")

    print("\n#3.2 : edge differences (cluster compositions)")

    visited_clusters = []
    similarity_edges = []

    for edge_diff_type in edge_diffs:
        for column_name in edge_diffs[edge_diff_type]:

            if column_name == 'discarded':
                graph = causality_graphs['non-faulty']['graph']
            else:
                graph = causality_graphs['faulty']['graph']

            for edge in edge_diffs[edge_diff_type][column_name]:

                if edge_diff_type == 'similarity':
                    similarity_edges.append(edge)

                for cluster in edge:

                    if edge[cluster] in visited_clusters:
                        continue

                    service_name = edge[cluster][0]
                    p_metric = edge[cluster][1]
                    # print("%s -> %s\n" 
                    #     % (str(edge[cluster]), str(graph.clusters[service_name]['cluster-table'][p_metric].other_metrics)))

                    visited_clusters.append(edge[cluster])

    rca_plots.draw_edge_differences(edge_diffs['similarity'], 'similarity')
    rca_plots.draw_edge_differences(edge_diffs['novelty'], 'novelty')

    cluster_novelty = pd.DataFrame(cluster_novelty, columns = ['Scope', 'Type', 'nr-clusters'])
    edge_diffs_stats = pd.DataFrame(edge_diffs_stats, columns = ['Similarity threshold', 'Edge diff.', 'nr-edges'])
    cluster_reduction_stats = pd.DataFrame(cluster_reduction_stats, columns = ['Similarity threshold', 'Type', 'nr'])
    print(cluster_reduction_stats)
    print(edge_diffs_stats)
    rca_plots.plot_clusters(cluster_novelty, edge_diffs_stats, cluster_reduction_stats)

    print("")


if __name__ == "__main__":

    # use an ArgumentParser for a nice CLI
    parser = argparse.ArgumentParser()

    # options (self-explanatory)
    parser.add_argument(
        "--gc-dirs", 
         help = """granger causality dirs for 
                the 2 cases to be compared, separated by ',' and prefixed 
                by 'faulty:' and 'non-faulty:'. e.g. '--gc-dirs \"non-faulty:/dir_1,faulty:/dir_2\".""")

    parser.add_argument(
        "--include-services", 
         help = """filter services by inclusion.""",
                action = "store_true")

    parser.add_argument(
        "--updated-services", 
         help = """list of service names, separated by ','. the differences 
                to be analyzed are limited to the ingress and egress nodes 
                directly connected to the services in this list. if set to 
                \"all\", all services are considered.""")

    parser.add_argument(
        "--gc-significance", 
         help = """significance for granger causality: only p-values below 
                this threshold will be picked (default 0.01).""")

    parser.add_argument(
        "--exclude-metrics", 
         help = """metrics to exclude, separated by '|'. e.g. 
                '--exclude-metrics \"rx_bytes|rx_packets|tx_bytes|tx_packets\"' 
                excludes edges w/ network metrics in them.""")

    parser.add_argument(
        "--filter-edges", 
         help = """only show 'added' or 'deleted' edges. by default, no filter 
                is applied (all edge types are shown).""")

    parser.add_argument(
        "--test", 
         help = """run script with example graph.""",
                action = "store_true")

    args = parser.parse_args()

    # the user can also exclude specific metrics, if the metric names are known
    updated_services = []
    if args.updated_services:
        updated_services = args.updated_services.split(",")

    if not updated_services:
        sys.stderr.write("""%s: [ERROR] please supply (at least 1) updated service.\n""" % sys.argv[0]) 
        parser.print_help()
        sys.exit(1)

    if args.test:
        run_demo(updated_services)
        sys.exit(1)

    # quit if a dir w/ causality files hasn't been provided
    if not args.gc_dirs:
        sys.stderr.write("""%s: [ERROR] please supply 2 dirs w/ .causality files as '--gc-dirs'\n""" % sys.argv[0]) 
        parser.print_help()
        sys.exit(1)

    causality_dirs = args.gc_dirs.split(",", 1)

    if len(causality_dirs) < 2 or (not causality_dirs[0] or not causality_dirs[1]):
        sys.stderr.write("""%s: [ERROR] forgot 1 causality dir in '--gc-dirs'?\n""" % sys.argv[0]) 
        parser.print_help()
        sys.exit(1)

    # leave out gc values with significance larger than specified. use default 
    # if not specified.
    if not args.gc_significance:
        args.gc_significance = GC_SIGNIFICANCE_DEFAULT

    # the dependency graph can get too crowded. to reduce the nr. of services 
    # shown in the graph we can either EXCLUDE services on a list, EXOR only 
    # INCLUDE the services specified in a list. these lists are controlled by 
    # global variables INCLUDE and EXCLUDE
    if args.include_services:
        SELECT_BY_EXCLUSION = False

    # the user can also exclude specific metrics, if the metric names are known
    excluded_metrics = []
    if args.exclude_metrics:
        excluded_metrics = args.exclude_metrics.split("|")

    if not args.filter_edges:
        args.filter_edges = None

    # causality graphs, indexed as 'faulty' and 'non-faulty'
    causality_graphs = defaultdict()
    
    start_time = time.time()
    for c_dir in causality_dirs:

        version = c_dir.split(":", 1)[0]
        gc_dir  = c_dir.split(":", 1)[1]

        print("%s::main() : building \"%s\" Graph..." % (sys.argv[0], version))

        causality_graphs[version] = defaultdict()
        causality_graphs[version]['dir'] = gc_dir
        causality_graphs[version]['graph'] = Graph()
        causality_graphs[version]['graph'].fill(gc_dir, args.gc_significance, excluded_services = EXCLUDE)

        print("%s::main() : \"%s\" Graph built in %s seconds\n" % (sys.argv[0], version, (time.time() - start_time)))
        print("%s::main() : \"%s\" Graph w/ %d services and %d edges\n" % (sys.argv[0], version, 
            causality_graphs[version]['graph'].stats['services'], causality_graphs[version]['graph'].stats['edges']))

    run_rca(
        causality_graphs, 
        updated_services,
        args.filter_edges, 
        excluded_metrics)
