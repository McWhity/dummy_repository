
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
from mni_in_situ_2016 import insitu
import pdb

### Usage: extract insitu-measurements and Sentinel-1 backscatter data into csv file for the year 2016 (need file mni_in_situ_2016.py)

### Gathering of in-situ measurements (soil moisture, LAI, height, VWC) within Panda dataframe
#------------------------------------------------------------------------------

path = '/media/nas_data/FINISHED_PROJECTS/2016_Wallerfing_Campaign/field_data/field_measurements/vegetation/csv'

# SM path
path_SM = '/media/nas_data/FINISHED_PROJECTS/2016_Wallerfing_Campaign/field_data/field_measurements/soil_moisture/data/data/2016_summer'

# field names
fields = ['100', '300']
# fields = ['508']
# ESU names
esus = ['high', 'low', 'med', 'mean']
# esus = ['mean']

esus = ['high', 'low']

# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

df_LAI = insitu(path,path_SM,fields,esus,save_path)
#------------------------------------------------------------------------------
pixel = ['_Field_buffer_30','','_buffer_30','_buffer_50','_buffer_100']
# pixel = ['_Field_buffer_30']
pixel = ['_buffer_100']
pixel = ['_Field_buffer_30','_buffer_100']


processed_sentinel = ['multi','norm_multi']
# processed_sentinel = ['norm_multi']
processed_sentinel = ['multi']

for processed_sentinel_data in processed_sentinel:

    for pixels in pixel:
        print(pixels)
        path_ESU = '/media/tweiss/Work/z_final_mni_data_2017/'
        name_shp = 'ESU_2016'+pixels+'.shp'
        name_ESU = 'ESU_2016'+pixels+'.tif'

        if pixels == '_Field_buffer_30':
            subprocess.call('gdal_rasterize -at -of GTiff -a layer -te 781387 5398747 787279 5404517 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)
        else:
            subprocess.call('gdal_rasterize -at -of GTiff -a ID -te 781387 5398747 787279 5404517 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)

        path_data = '/media/nas_data/Thomas/S1/processed/Wallerfing/step3'

        df_output = pd.DataFrame(columns=pd.MultiIndex(levels=[[],[]], codes=[[],[]]))

        for field in fields:

            for esu in esus:
                sentinel1_observations = kafka.Sentinel1_Observations.S1Observations(path_data, os.path.join(path_ESU, name_ESU), emulators={'VH': kafka.observation_operators.sar_observation_operator,'VV': kafka.observation_operators.sar_observation_operator})

                g = gdal.Open(os.path.join(path_ESU, name_ESU))
                state_mask = g.ReadAsArray().astype(np.int)

                if pixels == '_Field_buffer_30':
                    if field == '100':
                        mask_value = 1
                        state_mask = state_mask==mask_value
                    elif field == '300':
                        mask_value = 3
                        state_mask = state_mask==mask_value
                else:
                    if field == '100' and esu == 'high':
                        mask_value = 1
                        state_mask = state_mask==mask_value
                    elif field == '100' and esu == 'low':
                        mask_value = 2
                        state_mask = state_mask==mask_value
                    elif field == '300' and esu == 'high':
                        mask_value = 3
                        state_mask = state_mask==mask_value
                    elif field == '300' and esu == 'low':
                        mask_value = 4
                        state_mask = state_mask==mask_value
                    # elif field == '515' and esu == 'mean':
                    #     m = np.ma.array(state_mask,mask=((state_mask==1) | (state_mask==2) | (state_mask==3)))
                    #     state_mask = m.mask
                    # elif field == '508' and esu == 'mean':
                    #     m = np.ma.array(state_mask,mask=((state_mask==4) | (state_mask==5) | (state_mask==6)))
                    #     state_mask = m.mask
                    # elif field == '542' and esu == 'mean':
                    #     m = np.ma.array(state_mask,mask=((state_mask==7) | (state_mask==8) | (state_mask==9)))
                    #     state_mask = m.mask
                    # elif field == '319' and esu == 'mean':
                    #     m = np.ma.array(state_mask,mask=((state_mask==10) | (state_mask==11) | (state_mask==12)))
                    #     state_mask = m.mask
                    # elif field == '301' and esu == 'mean':
                    #     m = np.ma.array(state_mask,mask=((state_mask==13) | (state_mask==14) | (state_mask==15)))
                    #     state_mask = m.mask

                SM = []

                Height = []
                VWC = []

                Height2 = []
                VWC2 = []
                VWC_pro = []
                VWC_pro2 = []
                sigma_sentinel_vv = []
                sigma_sentinel_vh = []
                theta = []
                relativeorbit = []
                orbitdirection = []
                satellite = []
                dates = []
                drybiomass = []
                wetbiomass = []

                for i, tim in enumerate(sorted(sentinel1_observations.dates)):
                    if tim < datetime.datetime(2016, 1, 25) or tim > datetime.datetime(2016, 10, 20):
                        pass
                    else:
                        if tim >= df_LAI.index.min() and tim <= df_LAI.index.max():
                            try:
                                # pdb.set_trace()
                                data = sentinel1_observations.get_band_data(tim, 'vv_' + processed_sentinel_data)
                                data_vh = sentinel1_observations.get_band_data(tim, 'vh_' + processed_sentinel_data)

                                if esu == 'mean':
                                    df_add = df_LAI.filter(like=field)
                                    LAI.append(df_add.filter(like='LAI mean HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    SM.append(df_add.filter(like='SM 5cm').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    Height.append(df_add.filter(like='Height [cm] mean').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    VWC.append(df_add.filter(like='Water content total kg/m2 mean HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    VWC_pro.append(df_add.filter(like='Water content total [%]').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    drybiomass.append(df_add.filter(like='Dry biomass total kg/m2 mean HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    wetbiomass.append(df_add.filter(like='Wet biomass total kg/m2 mean HANTS').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])

                                else:
                                    df_add = df_LAI.filter(like=field).filter(like=esu)

                                    SM.append(df_add.filter(like='SM 5cm').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    Height.append(df_add.filter(like='Height').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    VWC.append(df_add.filter(like='Water content total kg/m2').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    VWC_pro.append(df_add.filter(like='Water content total [%]').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    drybiomass.append(df_add.filter(like='Dry biomass total kg/m2').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])
                                    wetbiomass.append(df_add.filter(like='Wet biomass total kg/m2').loc[tim.strftime("%Y-%m-%d %H:00:00")].values[0])

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

                                attributes = xarray.open_dataset(sentinel1_observations.date_data[tim])

                                relativeorbit.append(attributes.relativeorbit)
                                orbitdirection.append(attributes.orbitdirection)
                                satellite.append(attributes.satellite)

                                dates.append(tim)
                            except (KeyError, IndexError):
                                print('Error'+esu+field+str(tim)[0:10])

                df_output[field + '_' + esu, 'date'] = dates
                df_output[field + '_' + esu, 'sigma_sentinel_vv'] = sigma_sentinel_vv
                df_output[field + '_' + esu, 'sigma_sentinel_vh'] = sigma_sentinel_vh
                df_output[field + '_' + esu, 'theta'] = theta
                df_output[field + '_' + esu, 'relativeorbit'] = relativeorbit
                df_output[field + '_' + esu, 'orbitdirection'] = orbitdirection
                df_output[field + '_' + esu, 'satellite'] = satellite

                df_output[field + '_' + esu, 'SM'] = SM
                df_output[field + '_' + esu, 'Height'] = Height
                df_output[field + '_' + esu, 'VWC'] = VWC
                # df_output[field + '_' + esu, 'vh/vv'] = np.array(sigma_sentinel_vh) / np.array(sigma_sentinel_vv)
                df_output[field + '_' + esu, 'watercontentpro'] = VWC_pro
                df_output[field + '_' + esu, 'drybiomass'] = drybiomass
                df_output[field + '_' + esu, 'wetbiomass'] = wetbiomass


            # df_output[field + '_mean', 'date'] = dates
            # df_output[field + '_mean', 'LAI'] = LAI2
            # df_output[field + '_mean', 'Height'] = Height2
            # df_output[field + '_mean', 'VWC'] = VWC2
            # df_output[field + '_mean', 'watercontentpro'] = VWC_pro2
            # SM2 = df_output.filter(like=field).filter(like='SM').mean(axis=1)
            # df_output[field + '_mean', 'SM'] = SM2
            # vv = df_output.filter(like=field).filter(like='sigma_sentinel_vv').mean(axis=1)
            # vh = df_output.filter(like=field).filter(like='sigma_sentinel_vh').mean(axis=1)
            # df_output[field + '_mean', 'sigma_sentinel_vv'] = vv
            # df_output[field + '_mean', 'sigma_sentinel_vh'] = vh
            # df_output[field + '_mean', 'relativeorbit'] = relativeorbit
            # df_output[field + '_mean', 'orbitdirection'] = orbitdirection
            # df_output[field + '_mean', 'satellite'] = satellite
            # theta_mean = df_output.filter(like=field).filter(like='theta').mean(axis=1)
            # df_output[field + '_mean', 'theta'] = theta_mean

        df_output.to_csv(os.path.join(save_path, 'new_in_situ_s1'+processed_sentinel_data+pixels+'_2016_paper3.csv'), encoding='utf-8', sep=',', float_format='%.4f')

pdb.set_trace()
