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

class Extractor(object):

    def __init__(self, fs):
        self._fs = fs

    def __call__(self, df):
        fs = self._fs
        df[fs.features_as_array_] = df.ID.apply(self.extract)
        return df

    def sort(self, time, mag, mag_err):
        sort_mask = time.argsort()
        return time[sort_mask], mag[sort_mask], mag_err[sort_mask]

    def extract(self, oid):
        print("Extracting {}...".format(oid))
        fs = self._fs
        o_path = os.path.join('data', "lc", "{}.tar".format(oid))
        with tarfile.TarFile(o_path) as tfp:
            i_member = tfp.getmember("./{}.I.dat".format(oid))
            v_member = tfp.getmember("./{}.V.dat".format(oid))
            i_band = tfp.extractfile(i_member)
            v_band = tfp.extractfile(v_member)

            lc_i = np.loadtxt(i_band)
            lc_v = np.loadtxt(v_band)

            time_i, mag_i, mag_err_i = self.sort(lc_i[:,0], lc_i[:,1], lc_i[:,2])
            time_v, mag_v, mag_err_v = self.sort(lc_v[:,0], lc_v[:,1], lc_v[:,2])

            atime, amag, amag2, aerror, aerror2 = feets.preprocess.align(
                mag_i, mag_v, time_i, time_v, mag_err_i, mag_err_v)
            import ipdb; ipdb.set_trace()
            a=1




        #~ lc = np.loadtxt(lc_path)
        #~ time, mag, mag_err = lc[:,0], lc[:,1], lc[:,2]

        #~

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
    extractor = Extractor(fs)

    df = pd.read_pickle("data/ogle3_2bc.pkl")
    df = df[df.two_bands == True]
    #~ df = mp_apply(df, filter2Bands)

    extractor(df)

    #~ features = mp_apply(df, extractor)
    #~ features.to_pickle("features/features.pkl")
    #~ features.to_csv("features/features.csv", index=False)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    main()
