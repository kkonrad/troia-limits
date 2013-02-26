import glob
import json
import os
import sys

from itertools import groupby

import matplotlib.pyplot as plt


COLS = 2


def key_gen(strr):
    return lambda x: x[strr]


def plot_datas(dimx, dimy, idx, fig, datas, attr):
    data = sorted([(x['NUM_ASSIGNS'], x[attr]) for x in datas])
    xs, ys = [map(lambda x: x[i], data) for i in (0, 1)]
    ax = fig.add_subplot(dimx, dimy, idx)
    ax.plot(xs, ys, marker='o')
    ax.set_xscale('log')
    ax.set_yscale('log')
    if datas:
        ax.set_title("%s %s" % (datas[0]["ALGORITHM"], attr))


def gen_subplots(rows, row, fig, datas):
    dimx, dimy = rows, COLS
    idx = row * dimy + 1
    datas = list(datas)
    for i, attr in enumerate(("UPLOAD", "COMPUTE")):
        plot_datas(dimx, dimy, idx + i, fig, datas, attr)


def generate_chart(data):
    k_alg = key_gen("ALGORITHM")
    data.sort(key=k_alg)
    grouped = [(k, list(l)) for k, l in groupby(data, k_alg)]
    fig = plt.figure()
    rows, cols = len(grouped), COLS
    for i, (alg, datas) in enumerate(grouped):
        gen_subplots(rows, i, fig, datas)
    fig.tight_layout()
    fig.savefig("timing_algorithms.png")


def collect_files(folder):
    return list(glob.glob(os.path.join(folder, "*.json")))


def load_data(fname):
    with open(fname, 'r') as F:
        return json.load(F)


def main(args):
    files = [x for folder in args for x in collect_files(folder)]
    data = map(load_data, files)
    generate_chart(data)


if __name__ == '__main__':
    main(sys.argv)
