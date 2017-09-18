#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import bz2
import multiprocessing as mp

from six import StringIO

import pandas as pd

import requests


# =============================================================================
# CONSTANTS
# =============================================================================

PROCS = mp.cpu_count()

URL = "http://ogledb.astrouw.edu.pl/~ogle/CVS/sendobj.php?starcat={}"


# =============================================================================
# FUNCTIONS
# =============================================================================

def chunk_it(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return enumerate(out)


class Download(mp.Process):

    def __init__(self, idx, ids):
        super(Download, self).__init__()
        self.proc_idx = idx
        self.ids = ids
        self.tot = len(self.ids)

    def run(self):
        for idx, id in enumerate(self.ids):
            dest = "data/lc/{}.tar".format(id)
            if not os.path.exists(dest):

                r = requests.get(URL.format(id), stream=True)
                if r.status_code == 200:
                    with open(dest, 'wb') as f:
                        for chunk in r.iter_content(1024):
                            f.write(chunk)
            print "[Proc{}] Source {}/{}!".format(
                self.proc_idx, idx, self.tot)

# =============================================================================
# LOAD
# =============================================================================

def download(procs=PROCS):
    with bz2.BZ2File("data/ogle3.txt.bz2") as fp:
        src = fp.read()

    columns = src.splitlines()[6].split()[1:]
    df = pd.read_table(StringIO(src), skiprows=7, names=columns)
    procs = []

    for idx, ids in chunk_it(list(df.ID.values), PROCS):
        proc = Download(idx, ids)
        proc.start()
        procs.append(proc)
    for proc in procs:
        proc.join()

    df.to_pickle("data/ogle3.pkl")


if __name__ == "__main__":
    download()
