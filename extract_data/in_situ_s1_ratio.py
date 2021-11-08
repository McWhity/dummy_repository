
import subprocess

import kafka
# from kafka.input_output import Sentinel1_Observations
import os
import gdal
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import xarray
from mni_in_situ_2017 import insitu
import pdb
import datetime
import glob
import os
from collections import namedtuple

import gdal

import numpy as np

import osr

import scipy.sparse as sp

WRONG_VALUE = -999.0  # TODO tentative missing value

SARdata = namedtuple('SARdata',
                     'observations uncertainty mask metadata emulator')





def reproject_image(source_img, target_img, dstSRSs=None):
    """Reprojects/Warps an image to fit exactly another image.
    Additionally, you can set the destination SRS if you want
    to or if it isn't defined in the source image."""
    g = gdal.Open(target_img)
    geo_t = g.GetGeoTransform()
    x_size, y_size = g.RasterXSize, g.RasterYSize
    xmin = min(geo_t[0], geo_t[0] + x_size * geo_t[1])
    xmax = max(geo_t[0], geo_t[0] + x_size * geo_t[1])
    ymin = min(geo_t[3], geo_t[3] + y_size * geo_t[5])
    ymax = max(geo_t[3], geo_t[3] + y_size * geo_t[5])
    xRes, yRes = abs(geo_t[1]), abs(geo_t[5])
    if dstSRSs is None:
        dstSRS = osr.SpatialReference()
        raster_wkt = g.GetProjection()
        dstSRS.ImportFromWkt(raster_wkt)
    else:
        dstSRS = dstSRSs
    g = gdal.Warp('', source_img, format='MEM',
                  outputBounds=[xmin, ymin, xmax, ymax], xRes=xRes, yRes=yRes,
                  dstSRS=dstSRS)
    if g is None:
        raise ValueError("Something failed with GDAL!")
    return g


class S1Observations(object):
    """
    """

    def __init__(self, data_folder, state_mask,
                 emulators={'VV': 'SOmething', 'VH': 'Other'}):

        """

        """
        # 1. Find the files
        files = glob.glob(os.path.join(data_folder, '*.nc'))
        files.sort()
        self.state_mask = state_mask
        self.dates = []
        self.date_data = {}
        for fich in files:
            fname = os.path.basename(fich)
            # TODO Maybe filter files by metadata
            # (e.g. select ascending/descending passes)
            splitter = fname.split('_')
            this_date = datetime.datetime.strptime(splitter[5],
                                                   '%Y%m%dT%H%M%S')
            self.dates.append(this_date)
            self.date_data[this_date] = fich
        # 2. Store the emulator(s)
        self.emulators = emulators
        self.bands_per_observation = {}
        for the_date in self.dates:
            self.bands_per_observation[the_date] = 2 # 2 bands


    def _read_backscatter(self, obs_ptr):
        """
        Read backscatter from NetCDF4 File
        Should return a 2D array that should overlap with the "state grid"
        Input
        ------
        fname (string, filename of the NetCDF4 file, path included)
        polarisation (string, used polarisation)
        Output
        ------
        backscatter values stored within NetCDF4 file for given polarisation
        """
        backscatter = obs_ptr.ReadAsArray()
        return backscatter

    def _calculate_uncertainty(self, backscatter):
        """
        Calculation of the uncertainty of Sentinel-1 input data
        Radiometric uncertainty of Sentinel-1 Sensors are within 1 and 0.5 dB
        Calculate Equivalent Number of Looks (ENL) of input dataset leads to
        uncertainty of scene caused by speckle-filtering/multi-looking
        Input
        ------
        backscatter (backscatter values)
        Output
        ------
        unc (uncertainty in dB)
        """

        # first approximation of uncertainty (1 dB)
        unc = backscatter*0.05


        # need to find a good way to calculate ENL
        # self.ENL = (backscatter.mean()**2) / (backscatter.std()**2)

        return unc

    def _get_mask(self, backscatter):
        """
        Mask for selection of pixels
        Get a True/False array with the selected/unselected pixels
        Input
        ------
        this_file ?????
        Output
        ------
        """

        mask = np.ones_like(backscatter, dtype=np.bool)
        mask[backscatter == WRONG_VALUE] = False
        return mask

    def get_band_data(self, timestep, band):
        """
        get all relevant S1 data information for one timestep to get processing
        done
        Input
        ------
        timestep
        band
        Output
        ------
        sardata (namedtuple with information on observations, uncertainty,
                mask, metadata, emulator/used model)
        """

        if band == 0:
            polarisation = 'VV'
        elif band == 1:
            polarisation = 'VH'
        else:
            polarisation = band
        this_file = self.date_data[timestep]
        fname = 'NETCDF:"{:s}":{:s}'.format(this_file, polarisation)
        obs_ptr = reproject_image(fname, self.state_mask)
        observations = self._read_backscatter(obs_ptr)
        uncertainty = self._calculate_uncertainty(observations)
        mask = self._get_mask(observations)
        R_mat = np.zeros_like(observations)
        R_mat = uncertainty
        R_mat[np.logical_not(mask)] = 0.
        N = mask.ravel().shape[0]
        R_mat_sp = sp.lil_matrix((N, N))
        R_mat_sp.setdiag(1./(R_mat.ravel())**2)
        R_mat_sp = R_mat_sp.tocsr()
        emulator = 'out of order'
        # TODO read in angle of incidence from netcdf file
        # metadata['incidence_angle_deg'] =
        fname = 'NETCDF:"{:s}":{:s}'.format(this_file, "sigma0_vv_multi")
        obs_ptr = reproject_image(fname, self.state_mask)
        metadata = {'incidence_angle': obs_ptr.ReadAsArray()}
        sardata = SARdata(observations, R_mat_sp, mask, metadata, emulator)
        return sardata




### Gathering of in-situ measurements (soil moisture, LAI, height, VWC) within Panda dataframe
#------------------------------------------------------------------------------
# LAI path
path = '/media/nas_data/2017_MNI_campaign/field_data/field_measurements/vegetation/revised/csv'

# SM path
path_SM = '/media/nas_data/2017_MNI_campaign/field_data/field_measurements/soil_moisture/data_revised'

# field names
fields = ['301', '508', '542', '319', '515']
# fields = ['508']
# ESU names
esus = ['high', 'low', 'med', 'mean']
esus = ['mean']

# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

df_LAI = insitu(path,path_SM,fields,esus,save_path)
#------------------------------------------------------------------------------
pixel = ['_Field_buffer_30','','_buffer_30','_buffer_50','_buffer_100']
pixel = ['_Field_buffer_30']
# pixel = ['_buffer_50']

# processed_sentinel = ['multi','norm_multi']
# processed_sentinel = ['mulit']


for pixels in pixel:
    print(pixels)
    path_ESU = '/media/tweiss/Work/z_final_mni_data_2017/'
    name_shp = 'ESU'+pixels+'.shp'
    name_ESU = 'ESU'+pixels+'.tif'

    if pixels == '_Field_buffer_30':
        subprocess.call('gdal_rasterize -at -of GTiff -a ID -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)
    else:
        subprocess.call('gdal_rasterize -at -of GTiff -a FID_ -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)

    path_data = '/media/nas_data/Thomas/S1/processed/MNI_2017_new_final/step3'
    # path_data = '/media/nas_data/Thomas/S1/processed/MNI_2017_new_final/ratio'

    df_output = pd.DataFrame(columns=pd.MultiIndex(levels=[[],[]], codes=[[],[]]))

    for field in fields:

        for esu in esus:
            sentinel1_observations = S1Observations(path_data, os.path.join(path_ESU, name_ESU), emulators={'VH': kafka.observation_operators.sar_observation_operator,'VV': kafka.observation_operators.sar_observation_operator})

            g = gdal.Open(os.path.join(path_ESU, name_ESU))
            state_mask = g.ReadAsArray().astype(np.int)

            if pixels == '_Field_buffer_30':
                if field == '515':
                    mask_value = 4
                    state_mask = state_mask==mask_value
                elif field == '508':
                    mask_value = 27
                    state_mask = state_mask==mask_value
                elif field == '542':
                    mask_value = 8
                    state_mask = state_mask==mask_value
                elif field == '319':
                    mask_value = 67
                    state_mask = state_mask==mask_value
                elif field == '301':
                    mask_value = 87
                    state_mask = state_mask==mask_value
            else:
                if field == '515' and esu == 'high':
                    mask_value = 1
                    state_mask = state_mask==mask_value
                elif field == '515' and esu == 'med':
                    mask_value = 2
                    state_mask = state_mask==mask_value
                elif field == '515' and esu == 'low':
                    mask_value = 3
                    state_mask = state_mask==mask_value
                elif field == '508' and esu == 'high':
                    mask_value = 4
                    state_mask = state_mask==mask_value
                elif field == '508' and esu == 'med':
                    mask_value = 5
                    state_mask = state_mask==mask_value
                elif field == '508' and esu == 'low':
                    mask_value = 6
                    state_mask = state_mask==mask_value
                elif field == '542' and esu == 'high':
                    mask_value = 7
                    state_mask = state_mask==mask_value
                elif field == '542' and esu == 'med':
                    mask_value = 8
                    state_mask = state_mask==mask_value
                elif field == '542' and esu == 'low':
                    mask_value = 9
                    state_mask = state_mask==mask_value
                elif field == '319' and esu == 'high':
                    mask_value = 10
                    state_mask = state_mask==mask_value
                elif field == '319' and esu == 'med':
                    mask_value = 11
                    state_mask = state_mask==mask_value
                elif field == '319' and esu == 'low':
                    mask_value = 12
                    state_mask = state_mask==mask_value
                elif field == '301' and esu == 'high':
                    mask_value = 13
                    state_mask = state_mask==mask_value
                elif field == '301' and esu == 'med':
                    mask_value = 14
                    state_mask = state_mask==mask_value
                elif field == '301' and esu == 'low':
                    mask_value = 15
                    state_mask = state_mask==mask_value
                elif field == '515' and esu == 'mean':
                    m = np.ma.array(state_mask,mask=((state_mask==1) | (state_mask==2) | (state_mask==3)))
                    state_mask = m.mask
                elif field == '508' and esu == 'mean':
                    m = np.ma.array(state_mask,mask=((state_mask==4) | (state_mask==5) | (state_mask==6)))
                    state_mask = m.mask
                elif field == '542' and esu == 'mean':
                    m = np.ma.array(state_mask,mask=((state_mask==7) | (state_mask==8) | (state_mask==9)))
                    state_mask = m.mask
                elif field == '319' and esu == 'mean':
                    m = np.ma.array(state_mask,mask=((state_mask==10) | (state_mask==11) | (state_mask==12)))
                    state_mask = m.mask
                elif field == '301' and esu == 'mean':
                    m = np.ma.array(state_mask,mask=((state_mask==13) | (state_mask==14) | (state_mask==15)))
                    state_mask = m.mask

            sigma_sentinel_vv = []
            sigma_sentinel_vh = []
            theta = []
            relativeorbit = []
            orbitdirection = []
            satellite = []
            dates = []
            vv = dict()
            vh = dict()
            theta = dict()
            anisotropy=dict()

            for i, tim in enumerate(sorted(sentinel1_observations.dates)):
                if tim < datetime.datetime(2017, 3, 20) or tim > datetime.datetime(2017, 10, 20):
                    pass
                else:
                    if tim >= df_LAI.index.min() and tim <= df_LAI.index.max():
                        try:

                            data = sentinel1_observations.get_band_data(tim, 'sigma0_vv_multi')
                            data2 = sentinel1_observations.get_band_data(tim, 'sigma0_vh_multi')
                            data3 = sentinel1_observations.get_band_data(tim, 'theta')

                            observations = data.observations*1.
                            observations2 = data2.observations*1.
                            observations3 = data3.observations*1.

                            observations[~data.mask] = np.nan
                            observations[~state_mask] = np.nan

                            observations2[~data.mask] = np.nan
                            observations2[~state_mask] = np.nan

                            observations3[~data.mask] = np.nan
                            observations3[~state_mask] = np.nan

                            sigma_vv = observations
                            sigma_vh = observations2
                            thetas = observations3


                            vv[tim] = sigma_vv
                            vh[tim] = sigma_vh
                            theta[tim] = thetas


                            # attributes = xarray.open_dataset(sentinel1_observations[tim])

                            # relativeorbit.append(attributes.relativeorbit)
                            # orbitdirection.append(attributes.orbitdirection)
                            # satellite.append(attributes.satellite)

                            dates.append(tim)
                        except (KeyError, IndexError):
                            print('Error'+esu+field+str(tim)[0:10])

            np.save(os.path.join(save_path, 'vv'+pixels+field+'.npy'), vv)
            np.save(os.path.join(save_path, 'vh'+pixels+field+'.npy'), vh)
            np.save(os.path.join(save_path, 'theta'+pixels+field+'.npy'), theta)
            # df_output[field + '_' + esu, 'date'] = dates
            # df_output[field + '_' + esu, 'theta'] = theta
            # df_output[field + '_' + esu, 'relativeorbit'] = relativeorbit
            # df_output[field + '_' + esu, 'orbitdirection'] = orbitdirection
            # df_output[field + '_' + esu, 'satellite'] = satellite


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

# np.save(os.path.join(save_path, 'entropy'+pixels+'.npy'), entropy)
# np.save(os.path.join(save_path, 'alpha'+pixels+'.npy'), anisotropy)
# df_output.to_csv(os.path.join(save_path, 'entropy'+processed_sentinel_data+pixels+'.csv'), encoding='utf-8', sep=',', float_format='%.4f')

pdb.set_trace()
