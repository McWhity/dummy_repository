#!/usr/bin/env python
# coding: utf-8
#
# Copyright (c) 2018 T Ramsauer. All rights reserved.

"""
    Extract values for lat lon from netCDF file
    Copyright (C) 2018  Thomas Ramsauer
"""

import glob
import os
import xarray as xr
import pdb
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import pdb

__author__ = "Thomas Ramsauer"
__copyright__ = "Thomas Ramsauer"
__license__ = "gpl3"


def extract_poi(fn, lat, lon, verbose=True, write=False, suffix=None):

    try:
        lat = float(lat)
        lon = float(lon)
        assert any([(type(lat) == int) or (type(lat) == float)])
        assert any([(type(lon) == int) or (type(lon) == float)])
    except Exception as e:
        raise e

    try:
        if verbose:
            print(f"loading dataset {fn}..", end="")
        f = xr.open_dataset(fn)

        assert f.coords['lat'] is not None
        assert f.coords['lon'] is not None
        if verbose:
            print('done.')
            print(f"getting data for lat:{lat} lon:{lon}..", end="")
        res = f.sel(lat=lat, lon=lon, method='nearest')
        if write:
            fn_base = os.path.splitext(os.path.basename(fn))[0]
            if suffix:
                out_file = f"{fn_base}_{suffix}.nc"
                res.to_netcdf(out_file)
                if verbose:
                    print(f"Wrote file {out_file}!")
            else:
                out_file = f"{fn_base}_subset_{lat}_{lon}.nc"
                res.to_netcdf(out_file)
                if verbose:
                    print(f"Wrote file {out_file}!")

        if verbose:
            print("done.")

        return res

    except FileNotFoundError as e:
        raise e
    except KeyError as e:
        raise e


years = ['_2017','_2018']


def main():
    filename = ("/media/tweiss/Daten/RADOLAN_API_v1.0.0.nc")
    for year in years:
        if year == '_2017':
            sm301high = extract_poi(filename,48.311267495155334 , 11.690093586221337)
            sm301low = extract_poi(filename,48.309003710746765 , 11.690081600099802)
            sm301med = extract_poi(filename,48.309491202235222 , 11.688159210607409)

            sm508high = extract_poi(filename,48.25078678317368 ,  11.719030933454633)
            sm508low = extract_poi(filename,48.251360692083836 , 11.71728297136724)
            sm508med = extract_poi(filename,48.249399159103632 , 11.719873482361436)

            sm542high = extract_poi(filename,48.284000912681222 , 11.738448617979884)
            sm542low = extract_poi(filename,48.284318000078201 , 11.73987815156579)
            sm542med = extract_poi(filename,48.282870277762413 , 11.739336932078004)


            yyy = pd.to_datetime(sm301high.time.time.values)
            xxx = datetime.datetime.strptime('2017-03-20', '%Y-%m-%d')
            zzz = datetime.datetime.strptime('2017-07-30', '%Y-%m-%d')

            mask = (yyy > xxx) & (yyy < zzz)
            # plt.plot(yyy[mask],test.api[mask]/100)
            # plt.plot(yyy[mask],test2.api[mask]/100)

            # plt.ylim(0.1,0.4)
            df = pd.DataFrame(index = yyy[mask])
            df['sm301high'] = sm301high.api[mask]/100
            df['sm301low'] = sm301low.api[mask]/100
            df['sm301med'] = sm301med.api[mask]/100
            df['sm508high'] = sm508high.api[mask]/100
            df['sm508low'] = sm508low.api[mask]/100
            df['sm508med'] = sm508med.api[mask]/100
            df['sm542high'] = sm542high.api[mask]/100
            df['sm542low'] = sm542low.api[mask]/100
            df['sm542med'] = sm542med.api[mask]/100
            # df = pd.DataFrame(data = test.api[mask]/100,index = yyy[mask], columns=['sm'])
            df.to_csv('/media/tweiss/Daten/data_AGU/api'+year+'_radolan.csv')

        elif year == '_2018':
            sm317high = extract_poi(filename,48.28269426000 , 11.65873974000)
            sm317low = extract_poi(filename,48.28289031000 , 11.65731305000)
            sm317med = extract_poi(filename,48.28361819000 , 11.65872004000)

            sm525high = extract_poi(filename,48.24720553000 , 11.71357566000)
            sm525low = extract_poi(filename,48.24535506000 , 11.71603709000)
            sm525med = extract_poi(filename,48.24767458000 , 11.71604002000)


            yyy = pd.to_datetime(sm317high.time.time.values)
            xxx = datetime.datetime.strptime('2017-03-20', '%Y-%m-%d')
            zzz = datetime.datetime.strptime('2017-07-30', '%Y-%m-%d')

            mask = (yyy > xxx) & (yyy < zzz)
            # plt.plot(yyy[mask],test.api[mask]/100)
            # plt.plot(yyy[mask],test2.api[mask]/100)

            # plt.ylim(0.1,0.4)
            df = pd.DataFrame(index = yyy[mask])
            df['sm317high'] = sm317high.api[mask]/100
            df['sm317low'] = sm317low.api[mask]/100
            df['sm317med'] = sm317med.api[mask]/100
            df['sm525high'] = sm525high.api[mask]/100
            df['sm525low'] = sm525low.api[mask]/100
            df['sm525med'] = sm525med.api[mask]/100

            # df = pd.DataFrame(data = test.api[mask]/100,index = yyy[mask], columns=['sm'])
            df.to_csv('/media/tweiss/Daten/data_AGU/api'+year+'_radolan.csv')
        else:
            pass

if __name__ == '__main__':
    main()
