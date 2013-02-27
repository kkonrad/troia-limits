import glob
import json
import os
import sys

from itertools import groupby

import matplotlib.pyplot as plt

COLUMNS = ("UPLOAD", "COMPUTE", "UPL+COMP")
COLS = len(COLUMNS)


def key_gen(strr):
    return lambda x: x[strr]


def plot_datas(ax, datas, attr):
    data = sorted([(x['NUM_ASSIGNS'], x[attr]) for x in datas])
    xs, ys = [map(lambda x: x[i], data) for i in (0, 1)]
    ax.plot(xs, ys, marker='o')
    ax.set_xscale('log')
    ax.set_yscale('log')
    if datas:
        ax.set_title("%s %s" % (datas[0]["ALGORITHM"], attr))


def gen_subplots(axs, datas):
    datas = list(datas)
    for ax, attr in zip(axs, COLUMNS):
        plot_datas(ax, datas, attr)


def generate_chart(data):
    k_alg = key_gen("ALGORITHM")
    data.sort(key=k_alg)
    grouped = [(k, list(l)) for k, l in groupby(data, k_alg)]
    rows, cols = len(grouped), COLS
    fig, axs = plt.subplots(rows, cols, sharex=True, sharey=True)
    for ax_row, (alg, datas) in zip(axs, grouped):
        gen_subplots(ax_row, datas)
    fig.tight_layout()
    fig.savefig("timing_algorithms.png")


def collect_files(folder):
    return list(glob.glob(os.path.join(folder, "*.json")))


def load_data(fname):
    with open(fname, 'r') as F:
        return json.load(F)


def extend_data(d):
    d["UPL+COMP"] = d["UPLOAD"] + d["COMPUTE"]
    return d


def main(args):
    files = [x for folder in args for x in collect_files(folder)]
    data = map(extend_data, map(load_data, files))
    generate_chart(data)


if __name__ == '__main__':
    main(sys.argv)
