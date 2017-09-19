#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tarfile

import pandas as pd

from libs.mppandas import mp_apply


# =============================================================================
# CLASSES
# =============================================================================

class Filter2Bands(object):

    def __call__(self, df):
        df["two_bands"] = df.ID.apply(self.extract)
        return df

    def extract(self, oid):
        print("Filtering {}...".format(oid))
        o_path = os.path.join('data', "lc", "{}.tar".format(oid))
        try:
            with tarfile.TarFile(o_path) as tfp:
                tfp.getmember("./{}.I.dat".format(oid))
                tfp.getmember("./{}.V.dat".format(oid))
        except:
            return pd.Series({"two_bands": False})
        return pd.Series({"two_bands": True})




# =============================================================================
# FUNCTIONS
# =============================================================================

def main():
    if os.path.exists("data/ogle3_2bc.pkl"):
        return
    filter2Bands = Filter2Bands()
    df = pd.read_pickle("data/ogle3.pkl")
    df2bands = mp_apply(df, filter2Bands)
    df2bands.to_pickle("data/ogle3_2bc.pkl")
    df2bands.to_pickle("data/ogle3_2bc.csv")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    main()
