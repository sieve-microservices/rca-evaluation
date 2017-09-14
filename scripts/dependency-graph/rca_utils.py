import os
import metrics_utils as mu
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import collections

import metadata

from collections import defaultdict

APP_METRIC_DELIMITER = "|"

GC_LAGS = [
    "p_for_lag_1",
    "p_for_lag_2",
    "p_for_lag_3",
    "p_for_lag_4",
    "p_for_lag_5"
]

def first(iterable, default = None):
    for item in iterable:
        return item
    return default

def erase_last_line(filename):
    with open(filename, 'r') as _file:
        lines = _file.readlines()
        lines[-1] = ""
    with open(filename, 'w') as _file:
        _file.writelines(lines)

class Edge:

    def __init__(self, p_metric, c_metric, p_metric_score, lag_list, is_bidir = False):
        self.p_metric = p_metric
        self.c_metric = c_metric
        self.p_metric_score = p_metric_score
        # self.dot_str = mu.EdgeDotString(p_metric, c_metric, is_bidir)
        self.is_bidir = is_bidir
        self.lag_list = lag_list

    def __repr__(self):
        return ("""p_m=%s,c_m=%s""" % (self.p_metric, self.c_metric))

    def __hash__(self):
        return hash((self.p_metric, self.c_metric))

    def __eq__(self, other):
        try:
            return (self.p_metric, self.c_metric) == (other.p_metric, other.c_metric)
        except AttributeError:
            return NotImplemented

class Service_Node:

    def __init__(self, service_name):
        self.service_name = service_name
        self.total_edges = 0
        self.total_p_score = 0.0
        # self.dot_str = mu.NodeDotString(service_name)
        self.edge_list = []

    def __str__(self):
        return ("""service name = %s, total edges = %d, total p_score = %4.2f""" % (self.service_name, self.total_edges, self.total_p_score))

    def __repr__(self):
        return ("""service_name=%s,edge_list=%s""" % (self.service_name, str([e.__hash__() for e in self.edge_list])))

    def __hash__(self):
        return hash(self.service_name)

    def __eq__(self, other_service):
        try:
            return self.service_name == other_service.service_name
        except AttributeError:
            return NotImplemented

    def add_sub_label(self, sub_label = ""):
        self.dot_str.add_sub_label(sub_label)

    def change_style(self, color = "", style = ""):
        # change color and/or style of node...
        self.dot_str.change_style(color, style)
        # ... and edges
        for edge in self.edge_list:
            edge.dot_str.change_style(color, style)

class Service_Cluster:

    def __init__(self, cluster_id, service_name = '', rep_metric = '', other_metrics = []):    

        # FIXME: no idea what this is...
        self.cluster_id = cluster_id
        # name of service to which cluster belongs
        self.service_name = service_name
        # rep. metric of cluster
        self.rep_metric = rep_metric
        # list of cluster metrics
        self.other_metrics = other_metrics

        # stats for individual metrics
        self.metric_diffs = defaultdict(set)

    def __str__(self):
        return ("""service name = %s, rep. metric = %s""" 
            % (self.cluster_id, self.service_name, self.rep_metric))

    def get_similarity_score(self, other_cluster, metric = 'jaccard'):

        if metric == 'jaccard':

            intersection = set(self.other_metrics) & set(other_cluster.other_metrics)
            union = (set(self.other_metrics) | set(other_cluster.other_metrics))
            
            return float(len(intersection)) / float(len(union))

        elif metric == 'modified-jaccard':

            intersection = set(self.other_metrics) & set(other_cluster.other_metrics)
            union = set(other_cluster.other_metrics)
            
            return float(len(intersection)) / float(len(union))

        # else:
        #     print("Service_Cluster::get_similarity_score() : unknown metric (%s)" % (metric))

        return 0.0

    def get_metric_diffs(self, metric_diffs, exclude_diffs = [], only_diffs = []):

        exclude_diffs.append('total-change')

        for diff in metric_diffs:

            if (len(only_diffs) > 0) and (diff not in only_diffs):
                # print("Service_Cluster::get_metric_diffs() : skipping %s diff" % (diff))
                continue
            elif diff in exclude_diffs:
                continue

            self.metric_diffs[diff] |= (set(metric_diffs[diff]) & set(self.other_metrics))
            # print("Service_Cluster::get_metric_diffs() : %s.%s[%s] diff = %s" % (self.service_name, self.rep_metric, diff, str(self.metric_diffs[diff])))

class Graph:

    """ representation of a dependency graph.

        we follow an adjacency list approach, with separate ingress and egress 
        lists. this allows one to list all granger causality edges involving 
        a service s, by causality direction (i.e. 'ingress' or 'egress' edges).
    """

    def __init__(self, is_diff = False):

        # the ingress and egress adjacency lists. we use 2 lists to quickly 
        # obtain causality relationships for a service.
        self.edges = defaultdict()
        self.edges['ingress']  = defaultdict(list)
        self.edges['egress']   = defaultdict(list)
        # used for individual metric diff. operations
        self.metrics    = defaultdict(set)
        # used for cluster diff. operations
        self.clusters   = defaultdict()

        self.stats      = defaultdict(int)

    def fill(self, granger_causality_dir, 
        significance = 0.01,
        select_by_inclusion = False,
        included_services = [], 
        select_by_exclusion = True, 
        excluded_services = [],
        p_score_on = False,
        excluded_metrics = []):

        service_edges = defaultdict()
        services = set()

        # load granger causality data from granger_causality_dir
        data = metadata.load(granger_causality_dir)
        
        # read measurements from .tsv files
        for gc_file in os.listdir(granger_causality_dir):

            # data dir may contain files which doesn't contain granger causality 
            # data (e.g. metadata.json)
            if not gc_file.endswith("causality.tsv.gz"):
                continue

            # read the .tsv file
            df = pd.read_csv(os.path.join(granger_causality_dir, gc_file), sep="\t")
            # bug in causation : sometimes a causality line may appear twice
            df.drop_duplicates(subset=["perpetrator", "consequence"], inplace=True)
            # cycle through rows, identify meaningful relationships between metrics, 
            # and write them to the .dot and .pr files.
            rows = df.iterrows()

            while True:

                # read file line by line using next()
                try:
                    row_a = next(rows)[1]
                    row_b = next(rows)[1]

                except StopIteration:
                    break

                # separate the service from metric name on the first occurrence of 
                # APP_METRIC_DELIMITER (default is '|')
                service_a, metric_a = row_a.perpetrator.split(APP_METRIC_DELIMITER, 1)
                service_b, metric_b = row_a.consequence.split(APP_METRIC_DELIMITER, 1)

                # 4 ways to exclude service/metric relationships:
                #   1)  explicit exclusion (or inclusion) of at least 1 of the 
                #       services in the pair
                if select_by_exclusion:
                    if (service_a in excluded_services) or (service_b in excluded_services):
                        continue
                else:
                    if not any(service_a.split('_', 1)[0] in s for s in included_services) \
                        or not any(service_b.split('_', 1)[0] in s for s in included_services):

                        continue

                #   2)  the clustering algorithm wasn't able to cluster the metrics 
                #       for at least 1 of the services in the pair
                if (not mu.is_clustered(data, service_a)) \
                    or (not mu.is_clustered(data, service_b)):

                    continue

                #   3)  the metrics involved in the relationship have been explcitly 
                #       excluded
                if (metric_a.rstrip("-diff") in excluded_metrics) \
                    or (metric_b.rstrip("-diff") in excluded_metrics):

                    # print("skipped (excluded): %s => %s" % \
                    #     (metric_a, metric_b))

                    continue

                #   4)  if fancy p-scores are used for weighted pagerank, skip 
                #       the edges with metric for which 
                #       P_SCORE_LOWER < p-score < P_SCORE_UPPER, the so-called 
                #       (by myself...) 'uncertainty' band
                if p_score_on:
                    try:
                        metric_a_score = float(p_scores[ p_scores['metric'] \
                            == metric_a.rstrip("-diff") ]['score'])
                        metric_b_score = float(p_scores[ p_scores['metric'] \
                            == metric_b.rstrip("-diff") ]['score'])

                        if ( (metric_a_score < P_SCORE_UPPER ) and (metric_b_score > P_SCORE_LOWER) ):
                            continue

                    except TypeError:
                        print("what? " + metric_a + " " + metric_b)

                else:
                    metric_a_score = 1.0
                    metric_b_score = 1.0

                # an interaction between services a and b may be valid for 
                # different lags. we keep tack of all of them in a dict, indexed 
                # by the direction of the relationship between a and b.
                gc_lags = defaultdict(list)

                for gc_lag in GC_LAGS:

                    if row_a[gc_lag] < significance and row_b[gc_lag] < significance:

                        if service_a not in service_edges:
                            service_edges[service_a] = defaultdict(int)
                        service_edges[service_a][service_b] += 1

                        if service_b not in service_edges:
                            service_edges[service_b] = defaultdict(int)
                        service_edges[service_b][service_a] += 1

                        gc_lags['bidir'].append(gc_lag.lstrip("p_for_lag_"))

                        self.stats['edges'] += 1
                        services.add(service_a)
                        services.add(service_b)

                    elif row_a[gc_lag] < significance:

                        if service_a not in service_edges:
                            service_edges[service_a] = defaultdict(int)
                        service_edges[service_a][service_b] += 1

                        if service_b not in service_edges:
                            service_edges[service_b] = defaultdict(int)
                        service_edges[service_b][service_a] += 1

                        gc_lags['forward'].append(gc_lag.lstrip("p_for_lag_"))

                        self.stats['edges'] += 1
                        services.add(service_a)
                        services.add(service_b)

                    elif row_b[gc_lag] < significance:

                        if service_a not in service_edges:
                            service_edges[service_a] = defaultdict(int)
                        service_edges[service_a][service_b] += 1

                        if service_b not in service_edges:
                            service_edges[service_b] = defaultdict(int)
                        service_edges[service_b][service_a] += 1

                        gc_lags['back'].append(gc_lag.lstrip("p_for_lag_"))

                        self.stats['edges'] += 1
                        services.add(service_a)
                        services.add(service_b)

                    else:
                        self.stats['edges'] += 0
                        continue

                    # we now distribute the lag lists over the add_edge() 
                    # operations
                    for key, lag_list in gc_lags.iteritems():

                        # add (service, metric) tuples
                        # print("%s::build_dependency_graph() : adding metric tuple (%s, %s)" % (sys.argv[0], service_a, metric_a))
                        # print("%s::build_dependency_graph() : adding metric tuple (%s, %s)" % (sys.argv[0], service_b, metric_b))
                        # self.metrics.add((service_a, metric_a))
                        # self.metrics.add((service_b, metric_b))

                        # if key == 'bidir' and lag_list:

                        #     # add adjacency FROM a INTO b, with the appropriate 
                        #     # p-score. set the bi-direction flag.
                        #     self.add_edge(
                        #         service_a, metric_a, metric_a_score, \
                        #         service_b, metric_b, \
                        #         lag_list, \
                        #         True)

                        if key == 'forward' and lag_list:

                            self.add_edge(
                                service_a, metric_a, metric_a_score, \
                                service_b, metric_b, \
                                lag_list)

                        elif key == 'back' and lag_list:

                            self.add_edge(
                                service_b, metric_b, metric_b_score, \
                                service_a, metric_a, \
                                lag_list)

        # for head in service_edges:

        #     print("\nSERVICE : %s" % (head))
        #     for tail in service_edges[head]:
        #         print("\t%s : %d" % (tail, service_edges[head][tail]))

        self.stats['services'] = len(services)
        print(services)

        return self

    def add_edge(
        self, 
        perpetrator_service, perpertator_metric, perpertator_metric_score, 
        consequence_service, consequence_metric, 
        valid_lags, 
        is_bidirectional = False):

        """adds an dep. graph edge to each of the adjacency lists.

        keyword arguments:
            perpetrator_service         -- perpetrator service
            perpertator_metric          -- perpetrator (representative) metric
            perpertator_metric_score    -- the 'fancy' metric score of p_metric
            consequence_service         -- consequence service
            consequence_metric          -- consequence metric
            valid_lags                  -- list of lags for which the granger 
                                           causality relationship is valid
            is_bidir                    -- True if the perpetrator_metric and 
                                           consequence_metric cause each 
                                           other, False otherwise
        """

        # first, add the edge to the ingress adjacency list, i.e. indexed 
        # by consequence
        service_node = first(n for n in self.edges['ingress'][consequence_service] if n.service_name == perpetrator_service)

        if not service_node:
            service_node = Service_Node(perpetrator_service)
            self.edges['ingress'][consequence_service].append(service_node)

        service_node.total_edges += 1
        service_node.total_p_score += perpertator_metric_score

        # # generate a .dot string for the edge
        # new_edge_str = mu.DotString(perpetrator, p_metric, \
        #     consequence, c_metric, \
        #     ("both" if is_bidir else "forward"), ("blue" if is_bidir else "gray"))
        
        # create a new Edge object
        new_edge = Edge(perpertator_metric, consequence_metric, perpertator_metric_score, valid_lags, is_bidirectional)
        service_node.edge_list.append(new_edge)

        # update the egress adjacency list (i.e. indexed by perpetrator service)
        service_node = first(n for n in self.edges['egress'][perpetrator_service] if n.service_name == consequence_service)
        if not service_node:
            service_node = Service_Node(consequence_service)
            self.edges['egress'][perpetrator_service].append(service_node)

        service_node.total_edges += 1
        service_node.total_p_score += perpertator_metric_score

        # instead of copying the Edge object, append a pointer to it (the edge 
        # is the same anyway)
        service_node.edge_list.append(new_edge)

    def add_edges(
        self, 
        service_name, adj_service_name, 
        direction, 
        edge_list, 
        color = "", 
        style = ""):

        """adds a list of edges to a given service pair. 

        keyword arguments:
            service_name        -- service 
            adj_service_name    -- perpetrator (representative) metric
            direction           -- ingress or egress edges
            edge_list           -- the list of edges to add
            color               -- n/a
            style               -- n/a 
        """

        service_node = first(n for n in self.edges[direction][service_name] if n.service_name == adj_service_name)
        if not service_node:
            service_node = Service_Node(adj_service_name)
            self.edges[direction][service_name].append(service_node)

        service_node.total_edges += len(edge_list)
        service_node.total_p_score += sum(e.p_metric_score for e in edge_list)

        [e.dot_str.change_style(color, style) for e in edge_list]
        service_node.edge_list += edge_list

#        print("[%s] %s [%s] has now %d %s/%s edges (/%d)" % (node_name, ("<-" if direction == 'ingress' else "->"), adj_node_name, len(edge_list), color, style, len(node.edge_list)))

    # def list_unidir_edges(self, conseq, c_metric):

    #     """Returns all unidirectional ingress edges for a particular service 
    #     which 'end' of a particular metric.

    #     Keyword arguments:
    #         conseq          -- the service to search for ingress edges
    #         c_metric        -- 'end' metric for ingress edges
    #     """

    #     edge_list = defaultdict(list)

    #     for node in self.edges['ingress'][conseq]:
    #         for edge in node.edge_list:
    #             if (not edge.is_bidir) and edge.c_metric == c_metric:
    #                 edge_list[node.name].append(edge)

    #     return edge_list

    # def normalize_p_scores(self):

    #     for conseq, perps in self.edges['ingress'].iteritems():
    #         sum_p_scores = sum(n.total_p_score for n in perps)

    #         for n in perps:
    #             n.total_p_score = n.total_p_score / sum_p_scores

    def extract_service_metadata(self, service_name, metadata):

        # organization of self.clusters is as follows:
        #   -# self.clusters[<service_name>] -> { 'silhouette_score': <x.xxx>, 
        #                                         'cluster-table': [ list of Service_Cluster() objects ] }
        if service_name not in self.clusters:

            self.clusters[service_name] = defaultdict()
            # extract clusters of service_name from metadata
            silhouette_score, clusters = mu.get_cluster_metrics(metadata, service_name)

            # update the silhouette score for the clustering of 
            # service_name
            self.clusters[service_name]['silhouette-score'] = silhouette_score

            # create a new Service_Cluster() object for each 
            # cluster of service_name, as extracted from metadata
            self.clusters[service_name]['cluster-table'] = defaultdict()
            for cluster_id, cluster_metrics in clusters.iteritems():

                # FIXME: this is an ugly hack to fix the '|' problem
                cluster_metrics['rep_metric'] = cluster_metrics['rep_metric'].split("|", 1)[-1]
                cluster_metrics['other_metrics'] = [m.split("|", 1)[-1] for m in cluster_metrics['other_metrics']]

                new_cluster = Service_Cluster(  cluster_id, 
                                                service_name, 
                                                cluster_metrics['rep_metric'], 
                                                cluster_metrics['other_metrics'])

                self.clusters[service_name]['cluster-table'][cluster_metrics['rep_metric']] = new_cluster
                self.metrics[service_name] |= set(cluster_metrics['other_metrics'])        

    def extract_metadata(self, metadata, add_services = []):

        """extracts metrics & clusters of a dependency graph, from metadata info.

        keyword arguments:
            metadata    measurement metadata (from .json file)
        """

        add_services = set(add_services)

        for direction in ('ingress', 'egress'):
            for service_name in self.edges[direction]:
                self.extract_service_metadata(service_name, metadata)
                add_services.discard(service_name)

        for service_name in add_services:
                self.extract_service_metadata(service_name, metadata)

    def get_metric_diffs(self, other_graph):

        metric_diffs = defaultdict()
        totals = defaultdict(int)

        for service_name in self.metrics:

            if service_name not in metric_diffs:
                metric_diffs[service_name] = defaultdict(set)

            # if the service name only shows up for self, this only has 
            # 'new' metrics
            if service_name not in other_graph.metrics:
                metric_diffs[service_name]['new'] = self.metrics[service_name]
                continue

            # print("Graph::get_metric_diffs() : OTHER.%s : %s" % (service_name, other_graph.metrics[service_name]))
            # print("Graph::get_metric_diffs() : SELF.%s : %s" % (service_name, self.metrics[service_name]))

            # new metrics are present in self, but not in other
            metric_diffs[service_name]['new'] = (self.metrics[service_name] - other_graph.metrics[service_name])
            # discarded metrics are not in self, but are in other
            metric_diffs[service_name]['discarded'] = (other_graph.metrics[service_name] - self.metrics[service_name])
            # unchanged metrics are common to both
            metric_diffs[service_name]['unchanged'] = (self.metrics[service_name] & other_graph.metrics[service_name])

            totals['new']       += len(metric_diffs[service_name]['new'])
            totals['discarded'] += len(metric_diffs[service_name]['discarded'])
            totals['unchanged'] += len(metric_diffs[service_name]['unchanged'])

        return metric_diffs, totals

    def get_cluster_diffs(self, non_faulty_graph, metric_diffs_by_service = None):

        if metric_diffs_by_service is None:
            metric_diffs_by_service, totals = self.get_metric_diffs(non_faulty_graph)

        # we return the following statistics, in dictionary form:
        #
        #   -# silhouette scores    :  silhouette scores by service on both 
        #                              faulty and non-faulty versions
        #
        #   -# cluster similarity,  : we collect the similarity between faulty
        #      by service             and non-faulty versions, and vice-versa. 
        #                             by default, we use jaccard coefficients as the 
        #                             similarity score.
        #   
        #   why both? isn't it the same? no, it's not. say you have the 
        #   following set of clusters:
        #
        #   (F)aulty        : {c_1, c_2, c_3}
        #   (N)on (F)aulty  : {c_4, c_5, c_6}
        #
        #   if you compare F -> NF, you will have a similarity assignment for 
        #   each F cluster, but one (or more) clusters in NF might not 
        #   be assigned to any relationship. to guarantee assignments for each 
        #   NF cluster, we evaluate the similarity in the opposite direction, 
        #   i.e. NF -> F.
        #
        #   ok, why do you need to guarantee that? this is important to understand 
        #   which clusters in F are missing the discarded metrics. if we 
        #   only had the F -> NF assignments, we could have some NF clusters 
        #   unassigned, and as a consequence, their discarded metrics 
        #   would be left unassigned too. 
        #
        #   -# metric diffs         : for each cluster, return the number of 
        #                             new, discarded and unchanged metrics

        # cluster differences will be saved in a dict, indexed by 'service-name', 
        # with the following structure:
        #
        # .... [silhouette-score]: (faulty silh. score, non-faulty silh. score)
        #
        # .... [similarity]: similarity scores for clusters in-between versions
        #
        # .... .... [f-nf]: assignments from clusters in Faulty version to 
        #                   clusters in Non-faulty version
        # .... .... [nf-f]: assignments from clusters in Non-faulty version to 
        #                   clusters in Faulty version
        #
        # .... .... .... [cluster rep. metric]: a 2 tuple, indexed by the 
        #                                       rep metric of the cluster, which 
        #                                       includes the cluster most 
        #                                       similar to it in the 'other' 
        #                                       version and the sim. score.
        cluster_diffs = defaultdict()

        for service_name in metric_diffs_by_service:

            if service_name not in self.clusters:
                continue

            if service_name not in non_faulty_graph.clusters:

                for rep_metric_faulty in self.clusters[service_name]['cluster-table']:

                    cluster_faulty = self.clusters[service_name]['cluster-table'][rep_metric_faulty]
                    cluster_faulty.get_metric_diffs(metric_diffs_by_service[service_name], only_diffs = ['new', 'unchanged'])

                    if service_name not in cluster_diffs:
                        cluster_diffs[service_name] = defaultdict()

                    if 'similarity' not in cluster_diffs[service_name]:
                        cluster_diffs[service_name]['similarity'] = defaultdict()

                    if 'f-nf' not in cluster_diffs[service_name]['similarity']:
                        cluster_diffs[service_name]['similarity']['f-nf'] = defaultdict()

                    cluster_diffs[service_name]['similarity']['f-nf'][rep_metric_faulty] = (None, 0.0)
                    cluster_diffs[service_name]['silhouette-score'] = (self.clusters[service_name]['silhouette-score'], -1.0)

                continue

            if service_name not in cluster_diffs:
                cluster_diffs[service_name] = defaultdict()

            total_discarded = 0

            # save the silhouette scores as (faulty version, non-faulty version) 
            # tuple
            cluster_diffs[service_name]['silhouette-score'] = (self.clusters[service_name]['silhouette-score'], 
                                                                non_faulty_graph.clusters[service_name]['silhouette-score'])

            # collect similarities for the Faulty -> Non-faulty direction. as 
            # as such, we cycle through each cluster in the Faulty version, 
            # and find the one in the Non-faulty version which is most similar
            for rep_metric_faulty in self.clusters[service_name]['cluster-table']:

                # get the ServiceCluster object indexed by rep_metric_faulty
                cluster_faulty = self.clusters[service_name]['cluster-table'][rep_metric_faulty]

                # update the metric_diffs component of each cluster. metric_diffs 
                # holds a quick reference to the 'new', 'discarded' and 
                # 'unchanged' metrics held by a particular cluster
                cluster_faulty.get_metric_diffs(metric_diffs_by_service[service_name], only_diffs = ['new', 'unchanged'])
                # for diff in metric_diffs_by_service[service_name]:
                #     if diff == 'total-change':
                #         continue
                #     cluster_faulty.metric_diffs[diff] = (set(metric_diffs_by_service[service_name][diff]) & set(cluster_faulty.other_metrics))

                # initialize the similarity 2 tuple
                similarity_tuple = (None, 0.0)

                if 'similarity' not in cluster_diffs[service_name]:
                    cluster_diffs[service_name]['similarity'] = defaultdict()

                if 'f-nf' not in cluster_diffs[service_name]['similarity']:
                    cluster_diffs[service_name]['similarity']['f-nf'] = defaultdict()

                max_score = 0.0
                for rep_metric_non_faulty in non_faulty_graph.clusters[service_name]['cluster-table']:

                    cluster_non_faulty = non_faulty_graph.clusters[service_name]['cluster-table'][rep_metric_non_faulty]

                    # calculate the similarity score between the cluster on faulty 
                    # and non-faulty versions.
                    score = cluster_faulty.get_similarity_score(cluster_non_faulty, metric = "modified-jaccard")

                    if max_score < score:
                        # update the 3-tuple
                        similarity_tuple = (cluster_non_faulty, score)
                        # update the max. similarity score
                        max_score = score

                cluster_diffs[service_name]['similarity']['f-nf'][cluster_faulty.rep_metric] = similarity_tuple

            # collect similarities for the Non-faulty -> Faulty direction
            for rep_metric_non_faulty in non_faulty_graph.clusters[service_name]['cluster-table']:

                cluster_non_faulty = non_faulty_graph.clusters[service_name]['cluster-table'][rep_metric_non_faulty]

                if 'nf-f' not in cluster_diffs[service_name]['similarity']:
                    cluster_diffs[service_name]['similarity']['nf-f'] = defaultdict()

                similarity_tuple = (None, 0.0)
                max_score = 0.0
                for rep_metric_faulty in self.clusters[service_name]['cluster-table']:

                    cluster_faulty = self.clusters[service_name]['cluster-table'][rep_metric_faulty]
                    score = cluster_non_faulty.get_similarity_score(cluster_faulty, metric = "modified-jaccard")

                    if max_score < score:
                        similarity_tuple = (cluster_faulty, score)
                        max_score = score

                cluster_diffs[service_name]['similarity']['nf-f'][cluster_non_faulty.rep_metric] = similarity_tuple

                # we now set the discarded metrics on the Faulty version 
                # cluster which is most similar to the Non-faulty cluster. to 
                # do so, we take the intersection of the metrics in the 
                # Non-faulty cluster with the complete list of discarded metrics 
                # for service_name.
                if similarity_tuple[0] is not None:
                    similarity_tuple[0].metric_diffs['discarded'] |= (set(metric_diffs_by_service[service_name]['discarded']) & set(cluster_non_faulty.other_metrics))
            #         print("Graph::get_cluster_diffs() : discarded for %s.%s = %d" % (service_name, similarity_tuple[0].rep_metric, len(set(metric_diffs_by_service[service_name]['discarded']) & set(cluster_non_faulty.other_metrics))))
            #         total_discarded += len((set(metric_diffs_by_service[service_name]['discarded']) & set(cluster_non_faulty.other_metrics)))

            #     #     print("Graph::get_cluster_diffs() : NF -> F similarity %s.%s < %.2f > %s.%s" % (service_name, rep_metric_non_faulty, similarity_tuple[1], service_name, similarity_tuple[0].rep_metric))
            #     # else:
            #     #     print("Graph::get_cluster_diffs() : no similar cluster to NF %s.%s" % (service_name, rep_metric_non_faulty))

            # print("Graph::get_cluster_diffs() : discarded for %s = %d" % (service_name, total_discarded))

        return cluster_diffs

    def get_cluster_novelty_score(self, service_name, rep_metric):

        novelty_score = 0

        try:
            cluster = self.clusters[service_name]['cluster-table'][rep_metric]

            for metric_type in ['new', 'discarded']:

                if metric_type not in cluster.metric_diffs:
                    continue

                novelty_score += len(cluster.metric_diffs[metric_type])

        except KeyError:
            return novelty_score

        return novelty_score

    def extract_service_edges(self, service_name, relevant_services = []):

        # we first collect the edges which occur in both 
        # 'faulty' and 'non-faulty' versions. this will give us a list of 
        # horizontal relationships.

        # FIXME: the fact that we need to do this means there may be 
        # something wrong with the Graph() representation...
        service_edges = defaultdict()

        for adj_service in self.edges['egress'][service_name]:

            if adj_service.service_name not in relevant_services:
                continue

            for edge in adj_service.edge_list:

                if edge.p_metric not in service_edges:
                    service_edges[edge.p_metric] = defaultdict()

                if adj_service.service_name not in service_edges[edge.p_metric]:
                    service_edges[edge.p_metric][adj_service.service_name] = defaultdict()

                service_edges[edge.p_metric][adj_service.service_name][edge.c_metric] = { 'lags' : edge.lag_list, 'type' : 0 }
                # print("Graph::extract_service_edges() : %s.%s -> %s.%s" % (service_name, edge.p_metric, adj_service.service_name, edge.c_metric))

        return service_edges

    def get_edge_diffs(self, 
        other_graph, cluster_diffs = None, 
        relevant_services = [], similarity_threshold = 0.01, novelty_threshold = 1):

        # we're interested in analyzing 'horizontal' and 'vertical' relationships 
        # in the dependency graph, as shown below:
        #
        # <faulty-version>.service_a.p_cluster     - causality ? -> <faulty-version>.service_b.c_cluster
        #                 ^                                                      ^
        #            similarity ?                                           similarity ?
        #                 v                                                      v
        # <non-faulty-version>.service_a.p_cluster - causality ? -> <non-faulty-version>.service_b.c_cluster
        #
        # 'horizontal' relationships: edges in each version of the 
        # dependency graph (i.e. 'faulty' and 'non-faulty'). 
        # 'vertical' relationships: similarities between clusters from the 
        # same service, in-between versions.
        #
        # 'horizontal' relationships are filtered according to one (or more) 
        # of the following criteria:
        #
        #   -# services involved in the relationship belong to a list of 
        #      relevant services (e.g. the top n services regarding metric difference 
        #      analysis)
        #   -# existence of similar clusters in-between versions (over some 
        #      threshold t), for both ends of the relationship
        #   -# one of the clusters involved in the relationship has a high
        #      novelty score
        #
        # we have 4 possible combinations for the relationships at the top and 
        # bottom ('x' means no causal relation, '-' means a causality exists):
        #   -# (-, x) : relationship discarded
        #   -# (x, -) : new relationship
        #   -# (-, d) : lag ('d'elay) change
        #   -# (-, -) : unchanged relationship

        # if cluster differences haven't been extracted, do it now...
        if cluster_diffs is None:
            cluster_diffs = self.get_cluster_diffs(other_graph)

        # edge diffs. are reported in the format:
        #
        #   [<type>].{'perpetrator': (<service-name>, <metric>), 'consequence': (<service-name>, <metric>)}
        #
        # in which,
        #   -# type : 'new', 'discarded', 'unchanged' or 'lag-change'
        #   -# perpetrator : the service.cluster tuple which Granger causes 
        #      the consequence end of the edge
        #   -# consequece : the service.cluster tuple which is Granger caused 
        #      by the perpetrator end
        edge_diffs = defaultdict()
        edge_diffs['similarity'] = defaultdict(list)
        edge_diffs['novelty'] = defaultdict(list)

        # we build the list of edge differences, service by service, looking 
        # at each service's egress edges

        potential_services = set()

        for service_name in cluster_diffs:

            potential_services.add(service_name)

            # print("\n\nGraph::get_edge_diffs() : SERVICE: %s" % (service_name))

            if service_name not in relevant_services:
                print("Graph::get_edge_diffs() : service %s discarded (not in relevant_services)" % (service_name))
                continue

            # if service_name not in other_graph.edges['egress']:
            #     print("Graph::get_edge_diffs() : service %s discarded (no egress edges)" % (service_name))
            #     continue

            # we first collect the edges which occur in both 
            # 'faulty' and 'non-faulty' versions. this will give us a list of 
            # horizontal relationships.

            # FIXME: the fact that we need to do this means there may be 
            # something wrong with the Graph() representation...
            service_edges = defaultdict()
            service_edges['faulty'] = self.extract_service_edges(service_name, relevant_services)
            service_edges['non-faulty'] = other_graph.extract_service_edges(service_name, relevant_services)

            # now, for each horizontal relationship, we check if there is 
            # a corresponding vertical relationship.

            # we start by looking at 'faulty' -> 'non-faulty' horizontal 
            # relationships, which allows us to detect 'new', 'unchanged' and 
            # 'lag-change' edges.
            for p_metric in service_edges['faulty']:

                # we now take 2 types of metrics to measure the relevance of 
                # an edge, both related to the clusters at each endpoint:
                #
                #   -# similarity score     : it measures how similar a cluster 
                #                             is to a cluster in the 'non-faulty' version
                #   -# novelty_score        : measures how 'novel' a cluster is, 
                #                             in terms of 'new' and 'discarded' metrics 
                similarity_score = cluster_diffs[service_name]['similarity']['f-nf'][p_metric][1]
                novelty_score = self.get_cluster_novelty_score(service_name, p_metric)

                # if service_name == 'keystone':
                #     print("(1) NOVELTY SCORE (%s.%s) = %d" % (service_name, p_metric, novelty_score))

                if novelty_score < novelty_threshold:
                    continue

                if similarity_score < similarity_threshold:
                    continue

                # print("Graph::get_edge_diffs() : (p_metric) %s.%s : similarity_score = %.2f, novelty_score = %d" 
                #     % (service_name, p_metric, similarity_score, novelty_score))

                for adj_service_name in service_edges['faulty'][p_metric]:
                    for c_metric in service_edges['faulty'][p_metric][adj_service_name]:

                        if adj_service_name not in cluster_diffs:
                            # print("Graph::get_edge_diffs() : adj. service %s discarded (not in cluster diffs)" % (adj_service_name))
                            continue

                        similarity_score = cluster_diffs[adj_service_name]['similarity']['f-nf'][c_metric][1]
                        novelty_score = self.get_cluster_novelty_score(adj_service_name, c_metric)

                        edge_diffs_keys = set()
                        if (novelty_score >= novelty_threshold) and (similarity_score >= similarity_threshold):
                            edge_diffs_keys.add('similarity')

                        if novelty_score >= novelty_threshold:
                            edge_diffs_keys.add('novelty')

                        # so, now we know there's an horizontal relationship of 
                        # interest, the question now lies in the '?' at the 
                        # bottom, i.e. is there a causality relation between 
                        # <non-faulty-version>.service_a.p_cluster and <non-faulty-version>.service_b.c_cluster ?
                        #
                        # if there is, this is an 'unchanged' relation, if there 
                        # isn't, this is a 'new' causality relation.
                        # to understand this, we extract the <non-faulty-version> 
                        # clusters.
                        if cluster_diffs[service_name]['similarity']['f-nf'][p_metric][0] is None:
                            edge_diffs_keys.discard('similarity')
                            non_faulty_p_metric = ""
                        else:
                            non_faulty_p_metric = cluster_diffs[service_name]['similarity']['f-nf'][p_metric][0].rep_metric

                        if cluster_diffs[adj_service_name]['similarity']['f-nf'][c_metric][0] is None:
                            edge_diffs_keys.discard('similarity')
                            non_faulty_c_metric = ""
                        else:
                            non_faulty_c_metric = cluster_diffs[adj_service_name]['similarity']['f-nf'][c_metric][0].rep_metric

                        if len(edge_diffs_keys) < 1:
                            # print("Graph::get_edge_diffs() : (c_metric) %s.%s : similarity_score = %.2f, novelty_score = %d (%s)" 
                            #     % (adj_service_name, c_metric, similarity_score, novelty_score, edge_diffs_keys))
                            # print("Graph::get_edge_diffs() : edge %s.%s -> %s.%s discarded (no score)" % (service_name, p_metric, adj_service_name, c_metric))
                            continue

                        # if adj_service_name == 'keystone':
                        #     print(edge_diffs_keys)
                        #     print("(2) NOVELTY SCORE (%s.%s) = %d" % (adj_service_name, c_metric, novelty_score))

                        try:
                        
                            # this might generate a KeyError
                            non_faulty_lag_list = service_edges['non-faulty'][non_faulty_p_metric][adj_service_name][non_faulty_c_metric]['lags']
                            faulty_lag_list     = service_edges['faulty'][p_metric][adj_service_name][c_metric]['lags']

                            # check for differences in lag
                            if set(faulty_lag_list) != set(non_faulty_lag_list):

                                # print("Graph::get_edge_diffs() : 'lag change' edge detected between (%s.%s (%.2f)) -> (%s.%s (%.2f))" 
                                #     % ( service_name, p_metric, cluster_diffs[service_name]['similarity']['f-nf'][p_metric][1], 
                                #         adj_service_name, c_metric, cluster_diffs[adj_service_name]['similarity']['f-nf'][c_metric][1] ))

                                for key in edge_diffs_keys:
                                    edge_diffs[key]['lag-change'].append( 
                                        { 'perpetrator': (service_name, p_metric), 
                                          'consequence': (adj_service_name, c_metric) } )

                            else:

                                # print("Graph::get_edge_diffs() : 'unchanged' edge detected between (%s.%s (%.2f)) -> (%s.%s (%.2f))" 
                                #     % ( service_name, p_metric, cluster_diffs[service_name]['similarity']['f-nf'][p_metric][1], 
                                #         adj_service_name, c_metric, cluster_diffs[adj_service_name]['similarity']['f-nf'][c_metric][1] ))

                                for key in edge_diffs_keys:
                                    edge_diffs[key]['unchanged'].append( 
                                        { 'perpetrator': (service_name, p_metric), 
                                          'consequence': (adj_service_name, c_metric) } )
                        
                        except KeyError:

                            # print("Graph::get_edge_diffs() : 'new' edge detected between (%s.%s (%.2f)) -> (%s.%s (%.2f))" 
                            #     % ( service_name, p_metric, cluster_diffs[service_name]['similarity']['f-nf'][p_metric][1], 
                            #         adj_service_name, c_metric, cluster_diffs[adj_service_name]['similarity']['f-nf'][c_metric][1] ))

                            for key in edge_diffs_keys:
                                edge_diffs[key]['new'].append( 
                                    { 'perpetrator': (service_name, p_metric), 
                                      'consequence': (adj_service_name, c_metric) } )

            # finally, looking into 'non-faulty' -> 'faulty' allows us to 
            # detect 'discarded' edges. the reasoning is the same as above.
            for p_metric in service_edges['non-faulty']:

                if 'nf-f' not in cluster_diffs[service_name]['similarity']:
                    continue

                similarity_score = 0.0
                if p_metric in cluster_diffs[service_name]['similarity']['nf-f']:
                    similarity_score = cluster_diffs[service_name]['similarity']['nf-f'][p_metric][1]
                if (similarity_score < similarity_threshold):
                    # print("Graph::get_edge_diffs() : edge %s.%s -> ? discarded (no similarity score)" % (service_name, p_metric))
                    continue

                # print("Graph::get_edge_diffs() : (nf-f, p_metric) %s.%s : similarity_score = %.2f, novelty_score = %d" 
                #     % (adj_service_name, c_metric, similarity_score, novelty_score))

                for adj_service_name in service_edges['non-faulty'][p_metric]:
                    for c_metric in service_edges['non-faulty'][p_metric][adj_service_name]:

                        similarity_score = 0.0
                        if c_metric in cluster_diffs[adj_service_name]['similarity']['nf-f']:
                            similarity_score = cluster_diffs[adj_service_name]['similarity']['nf-f'][c_metric][1]
                        if (similarity_score < similarity_threshold):
                            # print("Graph::get_edge_diffs() : edge %s.%s -> %s.%s discarded (no similarity score)" % (service_name, p_metric, adj_service_name, c_metric))
                            continue

                        faulty_p_metric = cluster_diffs[service_name]['similarity']['nf-f'][p_metric][0].rep_metric
                        faulty_c_metric = cluster_diffs[adj_service_name]['similarity']['nf-f'][c_metric][0].rep_metric

                        p_novelty_score = self.get_cluster_novelty_score(service_name, faulty_p_metric)
                        c_novelty_score = self.get_cluster_novelty_score(service_name, faulty_c_metric)

                        if p_novelty_score < novelty_threshold or c_novelty_score < novelty_threshold:
                            continue

                        try:
                            # this might generate a KeyError
                            faulty_lag_list = service_edges['faulty'][faulty_p_metric][adj_service_name][faulty_p_metric]['lags']
                            non_faulty_lag_list  = service_edges['non-faulty'][p_metric][adj_service_name][c_metric]['lags']
                        
                        except KeyError:

                            # print("Graph::get_edge_diffs() : 'discarded' edge detected between (%s.%s (%.2f)) -> (%s.%s (%.2f))" 
                            #     % ( service_name, p_metric, cluster_diffs[service_name]['similarity']['nf-f'][p_metric][1], 
                            #         adj_service_name, c_metric, cluster_diffs[adj_service_name]['similarity']['nf-f'][c_metric][1] ))

                            edge_diffs['similarity']['discarded'].append( 
                                { 'perpetrator': (service_name, p_metric), 
                                  'consequence': (adj_service_name, c_metric) } )

        # and after all this giant mess, return
        return edge_diffs
