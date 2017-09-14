from itertools import izip_longest

APP_METRIC_DELIMITER = "|"

METRIC_STR_MAX_SIZE = 8
METRIC_STR_START_BIAS = 2

def is_clustered(data, service_name):

    for service in data["services"]:

        if service["name"] == service_name:
            pref_cluster_nr = service["pref_cluster"]
            if pref_cluster_nr == 0:
                return False
            else:
                return True

    return False

def get_cluster_metrics(data, service_name):

    """Extracts rep. metric and other metrics from the clusters of a service

    Keyword arguments:
    data            -- metadata from clustering
    service_name    -- name of service to extract metadata from
    """

    service_clusters = {}
    silhouette_score = ""

    for service in data["services"]:
        if service["name"] == service_name:
            # cycle through clusters, check which one has a 
            # 'grangercausality-metrics' key. this would be the 'chosen one'
            for cluster_size in service["clusters"]:

                cluster = service["clusters"][str(cluster_size)]

                if "rep_metrics" in cluster:
                    rep_metrics = {}
                    rep_metrics = cluster["rep_metrics"]
                    silhouette_score = cluster["silhouette_score"]

                    for cluster_id in cluster["metrics"]:

                        rep_metric = rep_metrics[str(cluster_id)]
                        cluster_metrics = cluster["metrics"][str(cluster_id)]

                        service_clusters[cluster_id] = {"rep_metric":rep_metric, "other_metrics":cluster_metrics}

    return silhouette_score, service_clusters

def format_metric_table_line(metrics, metrics_per_line, fillvalue=""):

    # the final result is a string, ready to add in between <TD></TD>
    metric_table_line = ""

    # we use izip_longest to iterate through 'metrics' by 'n' elements at a 
    # time (in this case 'mettrics_per_line')
    izip_longest_args = [iter(metrics)] * metrics_per_line
    for metric_line in izip_longest(*izip_longest_args, fillvalue=fillvalue):
        for metric_str in metric_line:
            # append the metric to the metric table line
            metric_table_line += strip_metric_str(str(metric_str)) + ","
        # add a line break to the table line
        metric_table_line = metric_table_line.rstrip(",") + "<BR/>"

    return(metric_table_line.rstrip("<BR/>"))

def get_metric_table_row(rep_metric, metric_differences, jaccard_scores, left=False, bg_color=None):

    if bg_color is not None:
        change_colors = {'added': 'black', 'deleted': 'black', 'swap-in': 'blue', 'swap-out': 'orange', 'remaining': 'black'}
    else:
        change_colors = {'added': 'darkgreen', 'deleted': 'red', 'swap-in': 'blue', 'swap-out': 'orange', 'remaining': 'black'}
        bg_color = "white"

    if not left:
        metric_table_row = """
                <TR>
                    <TD BGCOLOR="%s" PORT="%s">%s</TD>""" % (bg_color, strip_metric_str(rep_metric), strip_metric_str(rep_metric))

    else:
        metric_table_row = """
                <TR>
                    <TD BGCOLOR="%s">%s</TD>""" % (bg_color, strip_metric_str(rep_metric))

    metric_table_row += """
                    <TD BGCOLOR="%s">""" % (bg_color)

    for change, changed_metrics in metric_differences[rep_metric].iteritems():
        if len(changed_metrics) > 0:
            metric_table_row +="""<FONT COLOR="%s">%s</FONT>,\n""" % (change_colors[change], format_metric_table_line(changed_metrics, 2))

    if left:
        metric_table_row += """</TD>
                    <TD BGCOLOR="%s" PORT="%s">""" % (bg_color, strip_metric_str(rep_metric))

    else:

        metric_table_row += """</TD>
                    <TD BGCOLOR="%s">""" % (bg_color)

    if rep_metric in jaccard_scores:
        jaccard_score = float(jaccard_scores[rep_metric][0])
        closest_cluster = strip_metric_str(jaccard_scores[rep_metric][1])
    else:
        jaccard_score = 0.0
        closest_cluster = "N/A"

    metric_table_row += """%6.2f, %s</TD>
                </TR>""" % (jaccard_score, closest_cluster)

    return metric_table_row

def get_metric_table(service, clusters, with_diffs=False, left=False):

    if with_diffs:
        colspan = 3
    else:
        colspan = 2

    metric_table_str = ("""
    %s [
        shape=plaintext, 
        fontsize=8, 
        label=< 
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR>
                    <TD COLSPAN="%d">%s</TD>
                </TR>
                <TR>
                    <TD>R METRIC</TD>
                    <TD>OTHERS</TD>""" % (service, colspan, service))

    if with_diffs:
        metric_table_str += """
                    <TD>MAX. JACC. SCORE</TD>"""

    metric_table_str += """
                </TR>"""

    if with_diffs:

        for rep_metric in clusters.clusters:

            if rep_metric not in clusters.jaccard_scores:
                metric_table_str += get_metric_table_row(rep_metric, \
                                        clusters.metric_differences, \
                                        clusters.jaccard_scores, left, bg_color="lightgrey")

        for rep_metric in clusters.jaccard_scores:

            metric_table_str += get_metric_table_row(rep_metric, \
                                    clusters.metric_differences, \
                                    clusters.jaccard_scores, left)

        metric_table_str += """
            </TABLE>>]
    """

    else:

        # add rows for each representative metric + remaining cluster metrics
        for rep_metric, other_metrics in clusters.clusters.iteritems():

            if left:
                metric_table_str += """
                <TR>
                    <TD>%s</TD> 
                    <TD PORT="%s">%s</TD> 
                </TR>""" % (strip_metric_str(rep_metric), strip_metric_str(rep_metric), format_metric_table_line(other_metrics, 2))

            else:
                metric_table_str += """
                <TR>
                    <TD PORT="%s">%s</TD> 
                    <TD>%s</TD> 
                </TR>""" % (strip_metric_str(rep_metric), strip_metric_str(rep_metric), format_metric_table_line(other_metrics, 2))

        metric_table_str += """
            </TABLE>>]
    """

    return metric_table_str

def get_metric_table_diff(service, clusters_to_print, clusters_to_compare, left=False):

    metric_table_str = ("""
    %s [
        shape=plaintext, 
        fontsize=8, 
        label=< 
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR>
                    <TD COLSPAN="3">%s</TD>
                </TR>
                <TR>
                    <TD>R METRIC</TD>
                    <TD>OTHERS</TD>
                    <TD>MAX. JACC. SCORE</TD>
                </TR>""" % (service, service))

    jaccard_scores = clusters_to_print.get_jaccard_scores(clusters_to_compare)
    metric_differences, metric_change_score = clusters_to_print.get_metric_differences(clusters_to_compare, jaccard_scores)
    change_colors = {'added': 'darkgreen', 'deleted': 'red', 'swap-in': 'blue', 'swap-out': 'orange', 'remaining': 'black'}

    for rep_metric in clusters_to_print.clusters:

        if rep_metric not in jaccard_scores:
            metric_table_str += get_metric_table_row(rep_metric, \
                                    metric_differences, \
                                    jaccard_scores, left, bg_color="lightgrey")

    for rep_metric in jaccard_scores:

        metric_table_str += get_metric_table_row(rep_metric, \
                                metric_differences, \
                                jaccard_scores, left)

    metric_table_str += """
            </TABLE>>]
"""
    return metric_table_str

def strip_metric_str(metric_str):
    # strip metric prefixes and suffixes
    metric_str = metric_str.rstrip("-diff")
    metric_str = metric_str.split(APP_METRIC_DELIMITER, 1)

    if (len(metric_str) > 1):
        metric_str = metric_str[1]
    else:
        metric_str = metric_str[0]

    return metric_str

def abbreviate_metric_str(metric_str):
    # strip metric prefixes and suffixes
    metric_str = strip_metric_str(metric_str)

    # if after the strippin' the metric string size is still not 
    # appropriate for display, strip off the middle. 
    # we show more characters at the start of the string (nr. of characters 
    # controlled by METRIC_STR_START_BIAS. e.g. say the metric name is 
    # 'hotdogs_eaten_in_contest', METRIC_STR_MAX_SIZE is 8 and 
    # METRIC_STR_START_BIAS is 2. the string to 
    # show is 'hotdog::st' instead of 'hotd::test': same nr. of characters 
    # overall, but we show 2 more characters of the start of the string 
    # (and 2 less at the end).
    if len(metric_str) > (METRIC_STR_MAX_SIZE + 3):

        metric_str = \
            metric_str[0:((METRIC_STR_MAX_SIZE / 2) - 1 + METRIC_STR_START_BIAS)] \
                + "::" \
                + metric_str[-(METRIC_STR_MAX_SIZE / 2) - METRIC_STR_START_BIAS:]

    return metric_str
