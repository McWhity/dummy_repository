
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
import pdb
import glob
from pathlib import Path
import osr

def reproject_data(source_img,
        target_img=None,
        dstSRS=None,
        srcSRS=None,
        srcNodata=np.nan,
        dstNodata=np.nan,
        outputType=None,
        output_format="MEM",
        verbose=False,
        xmin=None,
        xmax=None,
        ymin=None,
        ymax=None,
        xRes=None,
        yRes=None,
        xSize=None,
        ySize=None,
        resample=1,
    ):

    """
    A method that uses a source and a target images to
    reproject & clip the source image to match the extent,
    projection and resolution of the target image.
    """

    outputType = (
        gdal.GDT_Unknown if outputType is None else outputType
        )
    if srcNodata is None:
            try:
                srcNodata = " ".join(
                    [
                        i.split("=")[1]
                        for i in gdal.Info(source_img).split("\n")
                        if " NoData" in i
                    ]
                )
            except RuntimeError:
                srcNodata = None
    # If the output type is intenger and destination nodata is nan
    # set it to 0 to avoid warnings
    if outputType <= 5 and np.isnan(dstNodata):
        dstNodata = 0

    if srcSRS is not None:
        _srcSRS = osr.SpatialReference()
        try:
            _srcSRS.ImportFromEPSG(int(srcSRS.split(":")[1]))
        except:
            _srcSRS.ImportFromWkt(srcSRS)
    else:
        _srcSRS = None


    if (target_img is None) & (dstSRS is None):
            raise IOError(
                "Projection should be specified ether from "
                + "a file or a projection code."
            )
    elif target_img is not None:
            try:
                g = gdal.Open(target_img)
            except RuntimeError:
                g = target_img
            geo_t = g.GetGeoTransform()
            x_size, y_size = g.RasterXSize, g.RasterYSize

            if xRes is None:
                xRes = abs(geo_t[1])
            if yRes is None:
                yRes = abs(geo_t[5])

            if xSize is not None:
                x_size = 1.0 * xSize * xRes / abs(geo_t[1])
            if ySize is not None:
                y_size = 1.0 * ySize * yRes / abs(geo_t[5])

            xmin, xmax = (
                min(geo_t[0], geo_t[0] + x_size * geo_t[1]),
                max(geo_t[0], geo_t[0] + x_size * geo_t[1]),
            )
            ymin, ymax = (
                min(geo_t[3], geo_t[3] + y_size * geo_t[5]),
                max(geo_t[3], geo_t[3] + y_size * geo_t[5]),
            )
            dstSRS = osr.SpatialReference()
            raster_wkt = g.GetProjection()
            dstSRS.ImportFromWkt(raster_wkt)
            gg = gdal.Warp(
                "",
                source_img,
                format=output_format,
                outputBounds=[xmin, ymin, xmax, ymax],
                dstNodata=dstNodata,
                warpOptions=["NUM_THREADS=ALL_CPUS"],
                xRes=xRes,
                yRes=yRes,
                dstSRS=dstSRS,
                outputType=outputType,
                srcNodata=srcNodata,
                resampleAlg=resample,
                srcSRS=_srcSRS
            )

    else:
            gg = gdal.Warp(
                "",
                source_img,
                format=output_format,
                outputBounds=[xmin, ymin, xmax, ymax],
                xRes=xRes,
                yRes=yRes,
                dstSRS=dstSRS,
                warpOptions=["NUM_THREADS=ALL_CPUS"],
                copyMetadata=True,
                outputType=outputType,
                dstNodata=dstNodata,
                srcNodata=srcNodata,
                resampleAlg=resample,
                srcSRS=_srcSRS
            )
    if verbose:
        LOG.debug("There are %d bands in this file, use "
                + "g.GetRasterBand(<band>) to avoid reading the whole file."
                % gg.RasterCount
            )
    return gg


def vwc_from_ndvi(ndvi, ndvimin, ndvimax, stemfactor):
    vwc = (1.9134 * ndvi**2 - 0.3215 * ndvi) + stemfactor * ((ndvimax-ndvimin)/(1-ndvimin))
    return vwc


# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

pixel = ['_Field_buffer_30']

for pixels in pixel:
    print(pixels)
    path_ESU = '/media/tweiss/Work/z_final_mni_data_2017/'
    name_shp = 'ESU'+pixels+'.shp'
    name_ESU = 'ESU'+pixels+'.tif'

    if pixels == '_Field_buffer_30':
        subprocess.call('gdal_rasterize -at -of GTiff -a ID -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)
    else:
        subprocess.call('gdal_rasterize -at -of GTiff -a FID_ -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)

    path_data = '/media/tweiss/Daten/NDVI/sentinel_2/nc'
    path = Path(path_data)
    g = gdal.Open(os.path.join(path_ESU, name_ESU))
    state_mask = g.ReadAsArray().astype(np.int)

    if pixels == '_Field_buffer_30':

        state_mask[state_mask==4] = 0
        state_mask[state_mask==67] = 0
        state_mask[state_mask>0] = 1

    files = glob.glob(os.path.join(path_data,'*.nc'))
    files.sort()
    data_ptr = {}
    minimum = {}
    maximum = {}
    date = []
    for file in files:
        fname = f'NETCDF:"'+file+'":newBand'
        xxx = reproject_data(fname, output_format="VRT", srcSRS="EPSG:4326", target_img=g)
        hm = xxx.ReadAsArray().astype(np.float)
        hm[state_mask==0] = np.nan
        data_ptr[file[-33:-25]] = hm

        minimum[file[-33:-25]] = np.nanmin(hm)
        maximum[file[-33:-25]] = np.nanmax(hm)
        date.append(file[-33:-25])

    vwc_ptr = {}
    for file in files:
        ndvi = data_ptr[file[-33:-25]]
        ndvimin = 0.1
        ndvimax = data_ptr[file[-33:-25]]
        stemfactor = 3.5
        vwc = vwc_from_ndvi(ndvi, ndvimin, ndvimax, stemfactor)
        vwc_ptr[file[-33:-25]] = vwc

    for file in files:
        plt.imshow(vwc_ptr[file[-33:-25]],vmin=0, vmax=5)
        plt.colorbar()
        plt.savefig('/media/tweiss/Daten/NDVI/sentinel_2/vwc_plot/'+file[-33:-25])
        plt.close()



# field names
fields = ['301', '508', '542']

esus = ['high', 'low', 'med']

pixel = ['_buffer_100']

for pixels in pixel:
    print(pixels)
    path_ESU = '/media/tweiss/Work/z_final_mni_data_2017/'
    name_shp = 'ESU'+pixels+'.shp'
    name_ESU = 'ESU'+pixels+'.tif'

    if pixels == '_Field_buffer_30':
        subprocess.call('gdal_rasterize -at -of GTiff -a ID -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)
    else:
        subprocess.call('gdal_rasterize -at -of GTiff -a FID_ -te 697100 5347200 703600 5354600 -tr 10 10 -ot Byte -co \"COMPRESS=DEFLATE\" '+path_ESU+name_shp+' '+path_ESU+name_ESU, shell=True)

    df_output = pd.DataFrame(columns=pd.MultiIndex(levels=[[],[]], codes=[[],[]]))
    for field in fields:
        for esu in esus:
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

                vwc_data = []

                for i in date:

                    vwc_data.append(np.nanmean(vwc_ptr[i][state_mask]))

                # df_output[field + '_' + esu, 'date'] = date
                df_output[field + '_' + esu, 'vwc_sentinel_2'] = vwc_data
    date = pd.to_datetime(date)
    df_output['', 'date'] = date
    df_output.set_index([('', 'date')], inplace=True)
    df_output.to_csv(os.path.join(save_path, 'vwc_sentinel_2'+pixels+'_2017_paper3.csv'), encoding='utf-8', sep=',', float_format='%.4f')

    df_output_2 = df_output.resample('D').mean().interpolate()
    df_output_2.to_csv(os.path.join(save_path, 'vwc_sentinel_2'+pixels+'_2017_paper3_2.csv'), encoding='utf-8', sep=',', float_format='%.4f')


pdb.set_trace()
