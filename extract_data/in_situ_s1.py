
import subprocess

import kafka
from kafka.input_output import Sentinel1_Observations
import os
import gdal
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import xarray
from mni_in_situ_2017 import insitu
import pdb

### Gathering of in-situ measurements (soil moisture, LAI, height, VWC) within Panda dataframe
#------------------------------------------------------------------------------
# LAI path
path = '/media/nas_data/2017_MNI_campaign/field_data/field_measurements/vegetation/revised/csv'

# SM path
path_SM = '/media/nas_data/2017_MNI_campaign/field_data/field_measurements/soil_moisture/data_revised'

# field names
fields = ['301', '508', '542', '319', '515']
fields = ['508']

# ESU names
esus = ['high', 'low', 'med']
esus = ['high']

# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

df_LAI = insitu(path,path_SM,fields,esus,save_path)
#------------------------------------------------------------------------------

path_ESU = '/media/tweiss/Work/z_final_mni_data_2017/'
name_shp = 'ESU.shp'
name_ESU = 'ESU.tif'

subprocess.call('gdal_rasterize -at -of GTiff -a FID_ -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)

path_data = '/media/nas_data/Thomas/S1/processed/MNI_2017/step3'
path_data = '/media/nas_data/Thomas/S1/step3'
processed_sentinel_data = 'multi'

df_output = pd.DataFrame(columns=pd.MultiIndex(levels=[[],[]], codes=[[],[]]))

for field in fields:

    for esu in esus:
        sentinel1_observations = kafka.Sentinel1_Observations.S1Observations(path_data, os.path.join(path_ESU, name_ESU), emulators={'VH': kafka.observation_operators.sar_observation_operator,'VV': kafka.observation_operators.sar_observation_operator})

        g = gdal.Open(os.path.join(path_ESU, name_ESU))
        state_mask = g.ReadAsArray().astype(np.int)

        if field == '515' and esu == 'high':
            mask_value = 1
        elif field == '515' and esu == 'med':
            mask_value = 2
        elif field == '515' and esu == 'low':
            mask_value = 3
        elif field == '508' and esu == 'high':
            mask_value = 4
        elif field == '508' and esu == 'med':
            mask_value = 5
        elif field == '508' and esu == 'low':
            mask_value = 6
        elif field == '542' and esu == 'high':
            mask_value = 7
        elif field == '542' and esu == 'med':
            mask_value = 8
        elif field == '542' and esu == 'low':
            mask_value = 9
        elif field == '319' and esu == 'high':
            mask_value = 10
        elif field == '319' and esu == 'med':
            mask_value = 11
        elif field == '319' and esu == 'low':
            mask_value = 12
        elif field == '301' and esu == 'high':
            mask_value = 13
        elif field == '301' and esu == 'med':
            mask_value = 14
        elif field == '301' and esu == 'low':
            mask_value = 15

        state_mask = state_mask==mask_value

        SM = []
        LAI = []
        Height = []
        VWC = []
        LAI2 = []
        Height2 = []
        VWC2 = []
        sigma_sentinel_vv = []
        sigma_sentinel_vh = []
        theta = []
        relativeorbit = []
        orbitdirection = []
        satellite = []
        dates = []

        for i, tim in enumerate(sorted(sentinel1_observations.dates)):
            if tim < datetime.datetime(2017, 1, 25) or tim > datetime.datetime(2017, 10, 20):
                pass
            else:
                if tim >= df_LAI.index.min() and tim <= df_LAI.index.max():
                    try:
                        data = sentinel1_observations.get_band_data(tim, 'vv_' + processed_sentinel_data)
                        data_vh = sentinel1_observations.get_band_data(tim, 'vh_' + processed_sentinel_data)

                        df_add = df_LAI.filter(like=field).filter(like=esu)


                        LAI.append(df_add.filter(like='LAI HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                        SM.append(df_add.filter(like='SM 5cm').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                        Height.append(df_add.filter(like='Height [cm]').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                        VWC.append(df_add.filter(like='Water content total kg/m2 HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])

                        df_add2 = df_LAI.filter(like=field)
                        LAI2.append(df_add2.filter(like='LAI mean HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                        Height2.append(df_add2.filter(like='Height [cm] mean').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                        VWC2.append(df_add2.filter(like='Water content total kg/m2 mean HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                        print(tim)

                        observations = data.observations*1.
                        observations_vh = data_vh.observations*1.

                        observations[~data.mask] = np.nan
                        observations[~state_mask] = np.nan
                        data.metadata['incidence_angle'][~data.mask] = np.nan
                        data.metadata['incidence_angle'][~state_mask] = np.nan

                        observations_vh[~data.mask] = np.nan
                        observations_vh[~state_mask] = np.nan
                        data_vh.metadata['incidence_angle'][~data.mask] = np.nan
                        data_vh.metadata['incidence_angle'][~state_mask] = np.nan
                        sigma_vv = observations
                        sigma_sentinel_vv.append(np.nanmean(sigma_vv))
                        sigma_vh = observations_vh
                        sigma_sentinel_vh.append(np.nanmean(sigma_vh))
                        theta.append(np.nanmean(data.metadata['incidence_angle']))
                        pdb.set_trace()
                        # relativeorbit.append(data.metadata['relativeorbit'])
                        # orbitdirection.append(data.metadata['orbitdirection'])
                        # satellite.append(data.metadata['satellite'])

                        dates.append(tim)
                    except (KeyError, IndexError):
                        pass

        df_output[field + '_' + esu, 'date'] = dates
        df_output[field + '_' + esu, 'sigma_sentinel_vv'] = sigma_sentinel_vv
        df_output[field + '_' + esu, 'sigma_sentinel_vh'] = sigma_sentinel_vh
        df_output[field + '_' + esu, 'theta'] = theta
        # df_output[field + '_' + esu, 'relativeorbit'] = relativeorbit
        # df_output[field + '_' + esu, 'orbitdirection'] = orbitdirection
        # df_output[field + '_' + esu, 'satellite'] = satellite
        df_output[field + '_' + esu, 'LAI'] = LAI
        df_output[field + '_' + esu, 'SM'] = SM
        df_output[field + '_' + esu, 'Height'] = Height
        df_output[field + '_' + esu, 'VWC'] = VWC
        df_output[field + '_' + esu, 'vh/vv'] = np.array(sigma_sentinel_vh) / np.array(sigma_sentinel_vv)
    df_output[field + '_mean', 'date'] = dates
    df_output[field + '_mean', 'LAI'] = LAI2
    df_output[field + '_mean', 'Height'] = Height2
    df_output[field + '_mean', 'VWC'] = VWC2
    SM2 = df_output.filter(like=field).filter(like='SM').mean(axis=1)
    df_output[field + '_mean', 'SM'] = SM2
    vv = df_output.filter(like=field).filter(like='sigma_sentinel_vv').mean(axis=1)
    vh = df_output.filter(like=field).filter(like='sigma_sentinel_vh').mean(axis=1)
    df_output[field + '_mean', 'sigma_sentinel_vv'] = vv
    df_output[field + '_mean', 'sigma_sentinel_vh'] = vh
    # df_output[field + '_mean', 'relativeorbit'] = relativeorbit
    # df_output[field + '_mean', 'orbitdirection'] = orbitdirection
    # df_output[field + '_mean', 'satellite'] = satellite
    theta_mean = df_output.filter(like=field).filter(like='theta').mean(axis=1)
    df_output[field + '_mean', 'theta'] = theta_mean

df_output.to_csv(os.path.join(save_path, 'in_situ_s1.csv'), encoding='utf-8', sep=',', float_format='%.4f')

pdb.set_trace()
