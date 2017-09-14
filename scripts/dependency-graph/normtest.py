import os, sys
import metadata
import pandas as pd
from plot import plt, sns
import scipy.stats as stats
from scipy.stats import shapiro, normaltest
from preprocess import load_timeseries

def draw(path):
    data = metadata.load(path)
    p_values_pearson = []
    p_values_shapiro = []
    norm_dist_path = os.path.join(path, "normtest_distribution.png")
    if os.path.exists(norm_dist_path):
        print("path exists %s, skip" % norm_dist_path)
        #return
    for srv in data["services"]:
        filename = os.path.join(path, srv["filename"])
        df = load_timeseries(filename, srv)
        columns = []
        for c in df.columns:
            if (not df[c].isnull().all()) and df[c].var() != 0:
                columns.append(c)
        df = df[columns]
        n = len(columns)
        if n == 0:
            continue
        fig, axis = plt.subplots(n, 2)
        fig.set_figheight(n * 4)
        fig.set_figwidth(30)

        for i, col in enumerate(df.columns):
            serie = df[col].dropna()
            sns.boxplot(x=serie, ax=axis[i, 0])
            statistic_1, p_value_1 = normaltest(serie)
            p_values_pearson.append(p_value_1)
            statistic_2, p_value_2 = shapiro(serie)
            p_values_shapiro.append(p_value_2)
            templ = """Pearson's normtest:
statistic: %f
p-value: %E
-> %s

Shapiro-Wilk test for normality:
statistic: %f
p-value: %E
-> %s
"""
            outcome_1 = "not normal distributed" if p_value_1 < 0.05 else "normal distributed"
            outcome_2 = "not normal distributed" if p_value_2 < 0.05 else "normal distributed"
            text = templ % (statistic_1, p_value_1, outcome_1, statistic_2, p_value_2, outcome_2)
            axis[i, 1].axis('off')
            axis[i, 1].text(0.05, 0.05, text, fontsize=18)
        plot_path = os.path.join(path, "%s_normtest.png" % srv["name"])
        plt.savefig(plot_path)
        print(plot_path)

    fig, axis = plt.subplots(2)
    fig.set_figheight(8)
    measurement = os.path.dirname(os.path.join(path,''))
    name = "Distribution of p-value for Pearson's normtest for %s" % measurement
    plot = sns.distplot(pd.Series(p_values_pearson, name=name), rug=True, kde=False, norm_hist=False, ax=axis[0])
    name = "Distribution of p-value for Shapiro-Wilk's normtest for %s" % measurement
    plot = sns.distplot(pd.Series(p_values_shapiro, name=name), rug=True, kde=False, norm_hist=False, ax=axis[1])
    fig.savefig(norm_dist_path)
    print(norm_dist_path)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurement" % sys.argv[0])
        sys.exit(1)
    draw(sys.argv[1])
