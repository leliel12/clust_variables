#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import warnings
import tarfile

import numpy as np

import pandas as pd

import feets

from libs.mppandas import mp_apply
from libs.ext_signature import Signature


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
        with tarfile.TarFile(o_path) as tfp:
            try:
                i_band = tfp.getmember("./{}.I.dat".format(oid))
                v_band = tfp.getmember("./{}.V.dat".format(oid))
            except:
                return pd.Series({"two_bands": False})
        return pd.Series({"two_bands": True})


class Extractor(object):

    def __init__(self, fs):
        self._fs = fs

    def __call__(self, df):
        fs = self._fs
        df[fs.features_as_array_] = df.ID.apply(self.extract)
        return df

    def extract(self, oid):
        #~ print("Extracting {}...".format(oid))
        fs = self._fs
        o_path = os.path.join('data', "lc", "{}.tar".format(oid))
        with tarfile.TarFile(o_path) as tfp:
            try:
                i_band = tfp.getmember("./{}.I.dat".format(oid))
                v_band = tfp.getmember("./{}.V.dat".format(oid))
            except:
                print tfp.getnames()



        #~ lc = np.loadtxt(lc_path)
        #~ time, mag, mag_err = lc[:,0], lc[:,1], lc[:,2]

        #~ sort_mask = time.argsort()
        #~ data = (mag[sort_mask], time[sort_mask], mag_err[sort_mask])

        #~ with warnings.catch_warnings():
            #~ warnings.simplefilter("ignore")
            #~ result = dict(zip(*fs.extract_one(data)))

        #~ return pd.Series(result)


# =============================================================================
# FUNCTIONS
# =============================================================================

def main():
    feets.register_extractor(Signature)
    fs = feets.FeatureSpace(
        data=["magnitude", "time", "error"],
        exclude=["SlottedA_length",
                 "StetsonK_AC",
                 "StructureFunction_index_21",
                 "StructureFunction_index_31",
                 "StructureFunction_index_32"])

    filter2Bands = Filter2Bands()
    extractor = Extractor(fs)

    df = pd.read_pickle("data/ogle3.pkl")
    df = mp_apply(df, filter2Bands)
    import ipdb; ipdb.set_trace()

    #~ extractor(df)

    #~ features = mp_apply(df, extractor)
    #~ features.to_pickle("features/features.pkl")
    #~ features.to_csv("features/features.csv", index=False)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    main()
