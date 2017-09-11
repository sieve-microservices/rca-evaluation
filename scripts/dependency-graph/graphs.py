import os
import sys

from plot import plt, sns

from cycler import cycler
import pandas as pd
from kshape import _sbd, lag

COLORS = [
  "#000000", "#FFFF00", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#006FA6", "#A30059",
  "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC", "#B79762", "#004D43", "#8FB0FF", "#997D87",
  "#5A0007", "#809693", "#1B4400", "#4FC601", "#3B5DFF", "#4A3B53", "#FF2F80",
  "#61615A", "#BA0900", "#6B7900", "#00C2A0", "#FFAA92", "#FF90C9", "#B903AA", "#D16100",
  "#DDEFFF", "#000035", "#7B4F4B", "#A1C299", "#300018", "#0AA6D8", "#013349", "#00846F",
  "#372101", "#FFB500", "#C2FFED", "#A079BF", "#CC0744", "#C0B9B2", "#C2FF99", "#001E09",
  "#00489C", "#6F0062", "#0CBD66", "#EEC3FF", "#456D75", "#B77B68", "#7A87A1", "#788D66",
  "#885578", "#FAD09F", "#FF8A9A", "#D157A0", "#BEC459", "#456648", "#0086ED", "#886F4C",
  "#34362D", "#B4A8BD", "#00A6AA", "#452C2C", "#636375", "#A3C8C9", "#FF913F", "#938A81",
  "#575329", "#00FECF", "#B05B6F", "#8CD0FF", "#3B9700", "#04F757", "#C8A1A1", "#1E6E00",
  "#7900D7", "#A77500", "#6367A9", "#A05837", "#6B002C", "#772600", "#D790FF", "#9B9700",
  "#549E79", "#FFF69F", "#201625", "#72418F", "#BC23FF", "#99ADC0", "#3A2465", "#922329",
  "#5B4534", "#FDE8DC", "#404E55", "#0089A3", "#CB7E98", "#A4E804", "#324E72", "#6A3A4C"
]

def docker_usage(path):
    data = metadata.load(path)
    for service in data["services"]:
        name = "%s/%s-usage.png" % (path, service["name"])
        if os.path.exists(name):
            print("skip " + name)
            continue
        filename = os.path.join(path, service["filename"])
        df = pd.read_csv(filename, sep="\t", index_col='time', parse_dates=True)
        if df["usage_percent"].size == 0:
            continue
        fields = ["usage_percent", "cache", "rx_bytes", "io_service_bytes_recursive_read", "io_serviced_recursive_write"]
        fig = df[fields].plot(subplots=True)
        plt.savefig(name)
        plt.close("all")

def cycle(offset=0):
    return cycler(color=COLORS[offset:])

def draw_series_combined(df, name, ax):
    ax.set_prop_cycle(cycle())
    df.plot(ax=ax)
    ax.set_title(name)
    ax.grid('on')
    ax.legend().remove()
    ax.set_ylabel('zscore')

def draw_series_seperate(df, ax):
    df2 = df.copy()
    for i, c in enumerate(df2.columns):
        df2[c] += 4 * i
    ax.set_title("Comparision")
    ax.set_prop_cycle(cycle())
    df2.plot(ax=ax)
    ax.yaxis.set_visible(False)
    ax.legend().remove()
    ax.set_ylim([-2, 4 * (len(df2.columns) + 1)])

def draw_sbd_dist_plot(distances, ax):
    if len(distances) > 1:
        sns.distplot(distances, ax=ax, kde=True)
    ax.set_title("distribution of shape based distance")
    ax.set_xlabel("bins")

def draw_sbd_bar_plot(distances, ax):
    ax.set_ylabel("shape based distance")
    ax.bar(range(len(distances)), distances, align='center', color=COLORS[1:])
    ax.set_xticks([])
    ax.set_xlabel("metrics")

def draw_lag(df, ax):
    lags = []
    for c in df.columns[1:]:
        lags.append(lag(df.centroid, df[c]))
    ax.set_ylabel("lag towards centroid")
    ax.bar(range(len(df.columns) - 1), lags, align='center', color=COLORS[1:])
    ax.set_xlabel("lag [500ms]")
    ax.set_xticks([])
    ax.set_xlabel("metrics")

def write(df, name):
    fig, axes = plt.subplots(ncols=2, nrows=3, figsize=(20,10))
    draw_series_combined(df, name, axes[0, 0])
    axes[0,1].axis('off')
    axes[0,1].legend(*axes[0,0].get_legend_handles_labels(), loc = 'upper left', ncol=2)

    draw_series_seperate(df, axes[2, 0])

    if df.centroid.notnull().any() and df.centroid.var() != 0:
        distances = []
        for c in df.columns[1:]:
            distances.append(_sbd(df.centroid, df[c])[0])

        draw_lag(df, axes[2,1])
        draw_sbd_bar_plot(distances, axes[1,0])
        draw_sbd_dist_plot(distances, axes[1,1])
    try:
        plt.tight_layout()
        plt.savefig(name, dpi=200)
        plt.close("all")
    except Exception as e:
        import pdb; pdb.set_trace()
        print("graph %s failed %s" % (name, e))

def draw_graph(name):
    df = pd.read_csv(name, sep="\t", index_col='time', parse_dates=True)
    write(df, os.path.splitext(name)[0] + ".png")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s centroid\n" % sys.argv[0])
        sys.exit(1)
    for name in sys.argv[1:]:
        draw_graph(name)
        print(name)
    #docker_usage(path)
