import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import os
import argparse
import sys
import glob
import metrics_utils as mu
import seaborn as sns

from datetime import date
from datetime import datetime
from collections import defaultdict
from collections import OrderedDict

matplotlib.rcParams.update({'font.size': 16})

FONTSIZE_LEGEND = 14

def get_print_label(service_name, sep_char = " "):

    # replace '_' w/ ' '
    print_label = service_name.replace("_", sep_char).capitalize()

    # special cases : full capitalization
    to_caps = ['mq', 'api', 'dhcp', 'ssh', 'l3']
    for cap in to_caps:

        if cap in print_label:
            print_label = print_label.replace(cap, cap.upper())

    return print_label

def get_dot_node(label, sub_label = '', color = 'black', style = 'solid'):

    return("%s [label=\"%s\\n%s\",color=%s,style=%s]\n" 
        % (label, get_print_label(label, "\n"), sub_label, color, style))

def get_dot_edge(head_node, tail_node, head_label, tail_label, 
    print_labels = False,
    direction = 'forward', color = 'black', style = 'solid'):

    # if the user desires to print labels on the graph edges directly, 
    # otherwise stick with tooltips
    if print_labels:
        return ('    "%s" -> "%s" [headlabel="%s",taillabel="%s",dir=%s,color=%s,style=%s]\n' \
            % (head_node, tail_node, \
                mu.abbreviate_metric_str(tail_label), mu.abbreviate_metric_str(head_label), \
                direction, color, style))
    else:
        return ('    "%s" -> "%s" [tooltip="%s > %s",dir=%s,color=%s,style=%s]\n' \
            % (head_node, tail_node, \
                mu.abbreviate_metric_str(head_label), mu.abbreviate_metric_str(tail_label), \
                direction, color, style))

def to_latex_table(data, data_indexes = None, sub_keys = None):

    if data_indexes is None:
        data_indexes = data.keys()

    for key in data_indexes:
        row = get_print_label(key)

        if sub_keys is None:
            sub_keys = data[key].keys()

        for sub_key in sub_keys:
            row += (" \t& %d" % (len(data[key][sub_key])))

        print("%s \\\\" % (row))

def draw_edge_differences(data, edge_diff_type = "none", graph_dir = "rca-evaluation/graphs", filename = "rca-evaluation-edge-diffs"):

    with open(os.path.join(graph_dir, (filename + "-" + edge_diff_type)) + ".dot", "w+") as dot_file:

        dot_file.write("""
digraph {
overlap = false;
pack = false;
splines = curved;
rankdir = "LR";
nodesep = 0.35;
graph [ dpi = 300 ]; 
node [ fontsize = 8 ];
edge [ fontsize = 6 ];
""")

        # keep track of added services
        added_services = []
        added_edges = []
        edge_colors = {'new': 'green', 'discarded': 'red', 'lag-change': 'blue'}
        edge_styles = {'new': 'solid', 'discarded': 'dashed', 'lag-change': 'dotted'}

        for edge_type in data:

            if edge_type == 'unchanged':
                continue

            for edge in data[edge_type]:

                # extract (service, metric) tuples
                p_service = edge['perpetrator'][0]
                p_metric  = edge['perpetrator'][1]
                c_service = edge['consequence'][0]
                c_metric  = edge['consequence'][1]

                if (p_service, c_service, edge_type) not in added_edges:
                    added_edges.append((p_service, c_service, edge_type))
                else:
                    continue

                # write .dot nodes
                if p_service not in added_services:
                    dot_file.write(get_dot_node(p_service))
                    added_services.append(p_service)

                if c_service not in added_services:
                    dot_file.write(get_dot_node(c_service))
                    added_services.append(c_service)

                # write .dot edges
                dot_file.write(get_dot_edge(p_service, c_service, p_metric, c_metric, 
                    print_labels = False, color = edge_colors[edge_type], style = edge_styles[edge_type]))

        # wrap and close the main .dot file
        dot_file.write("}")

def autolabel(rects, ax):
    """
    attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')

def add_abs_values(ax):

    for p in ax.patches:
        height = p.get_height()
        ax.text(p.get_x()+p.get_width()/2.,
                height + 3,
                '{:d}'.format(int(height)),
                ha="center",
                fontsize = 16)

def rescale_barplot_width(ax, factor=0.6):
    for bar in ax.patches:
        x = bar.get_x()
        new_width = bar.get_width() * factor
        center = x + bar.get_width() / 2.
        bar.set_width(new_width)
        bar.set_x(center - new_width / 2.)

def plot_clusters(
    novelty_data, edge_diff_stats, cluster_reduction_stats, 
    figure_dir = "rca-evaluation/figures", filename = "rca-evaluation-clusters"):

    plt.rc('text', usetex = True)
    sns.set_context(font_scale = 1.5)
    sns.set(font_scale = 2.0)
    # seq_col_brew = sns.color_palette("Greys_r", 5)
    # sns.set_palette(seq_col_brew)

    # labels and colors for novelty bar chart
    # novelty_labels = ['New', 'Discarded', 'New & Discarded', 'Total']
    # novelty_colors = ['green', 'red', 'blue', 'black']
    novelty_labels = ['New', 'Discarded', 'New & Disc.', 'Total']
    novelty_colors = ['black', 'black', 'black', 'black']

    # FIXME: is this the style used by joerg?
    # matplotlib.style.use('classic')
    fig = plt.figure(figsize=(20, 2.75))

    # ax1 is the bar chart
    sns.set_style("whitegrid")
    ax1 = fig.add_subplot(131)
    # ax1.xaxis.grid(False)
    # ax1.yaxis.grid(True)

    show_legend = True

    # plot the bar chart, one bar at a time...
    # sns.barplot(x = 'Type', y = 'nr-clusters', hue = 'Scope', data = novelty_data, 
    #     linewidth = 0.0, estimator = sum, ci = None)
    sns.barplot(x = 'Type', y = 'nr-clusters', data = novelty_data, 
        linewidth = 0.0, estimator = sum, ci = None, color = '#444444')
    rescale_barplot_width(ax1, 0.8)

    # for novelty_label in ['new', 'discarded', 'new-and-discarded', 'total']:
    #     # rect = ax1.bar(x, novelty_data[novelty_label], 
    #     #     color = novelty_colors[x], linewidth = 0.00, alpha = 1.00, width = 0.80, label = novelty_labels[x])
    #     rect = sns.barplot(x, novelty_data[novelty_label], 
    #         color = novelty_colors[x], linewidth = 0.00, alpha = 1.00, width = 0.80, label = novelty_labels[x])

    #     rects.append([r for r in rect][0])

    #     x += 1

    add_abs_values(ax1)
    # autolabel(rects, ax1)

    ax1.set_title("a) Cluster novelty", fontsize = 22)
    ax1.set_xlabel("")
    ax1.set_ylabel("\# of clusters", fontsize = 22)
    # x axis
    # ax1.set_xlim(-0.2, 4)
    # ax1.set_xticks([0.4, 1.4, 2.4, 3.4])
    # ax1.set_xticklabels(novelty_labels, fontsize = 14, rotation=25)
    # y axis
    # ax1.set_yscale('log')
    ax1.set_ylim(0, 80)
    ax1.set_yticks([0, 20, 40, 60, 80])
    ax1.legend(fontsize = 20, ncol=1, loc='upper left')
    # ax1.set_yticklabels(['0', '10', '20', '30', '40', '50', '60', '70'], fontsize = 14)
    # legend
    # ax1.legend(fontsize=FONTSIZE_LEGEND, ncol=1, loc='upper left')

    seq_col_brew = sns.color_palette("Greys_r", 4)
    sns.set_palette(seq_col_brew)
    ax2 = fig.add_subplot(132)
    # ax2.xaxis.grid(True)
    # ax2.yaxis.grid(True)

    # plot the bar chart, one bar at a time...
    sns.barplot(x = 'Similarity threshold', y = 'nr-edges', hue = 'Edge diff.', data = edge_diff_stats, 
        linewidth = 0.0, estimator = sum, ci = None)
    rescale_barplot_width(ax2, 0.8)
    add_abs_values(ax2)

    ax2.set_title("b) Edge novelty", fontsize = 22)
    ax2.set_xlabel("Similarity threshold", fontsize = 22)
    ax2.set_ylabel("\# of edges", fontsize = 22)

    ax2.legend(fontsize = 18, ncol=1, loc='upper right')
    ax2.set_ylim(0, 60)
    ax2.set_yticks([0, 10, 20, 30, 40, 50, 60])

    seq_col_brew = sns.color_palette("Greys_r", 3)
    sns.set_palette(seq_col_brew)
    ax3 = fig.add_subplot(133)

    sns.barplot(x = 'Similarity threshold', y = 'nr', hue = 'Type', data = cluster_reduction_stats, 
        linewidth = 0.0, estimator = sum, ci = None)
    rescale_barplot_width(ax3, 0.8)
    add_abs_values(ax3)

    ax3.set_title("c) \# of services, clusters and metrics", fontsize = 22)
    ax3.set_xlabel("Similarity threshold", fontsize = 22)
    ax3.set_ylabel("\# services, clusters\nand metrics", fontsize = 22)

    ax3.set_ylim(0, 350)
    ax3.legend(fontsize = 18, ncol=1, loc='upper right')
    ax3.set_yticks([0, 100, 200, 300])

    fig.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.3, hspace=None)
    plt.savefig(os.path.join(figure_dir, filename) + ".pdf", bbox_inches='tight', format = 'pdf')
