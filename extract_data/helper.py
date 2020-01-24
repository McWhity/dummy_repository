
import os
import glob
from sense import model

def filter_relativorbit(data, field, orbit1, orbit2=None, orbit3=None, orbit4=None):
    """ data filter for relativ orbits """
    output = data[[(check == orbit1 or check == orbit2 or check == orbit3 or check == orbit4) for check in data[(field,'relativeorbit')]]]
    return output

def list_files(inputfolder, expression=None):
    """Create list containing all files in inputfolder (without subfolder) that contain expression in the name"""
    if expression is None:
        expression = '*'
    filelist = glob.glob(os.path.join(inputfolder,expression))
    print("Number of found files:", len(filelist))
    return filelist

def run_SSRT(surface, vegetation, SAR):
    """
    implementation of Oh model and Single Scattering RT-model
    Return Polarization VV and VH in linear scale
    """

    clay, sand, bulk, sm, s = surface
    d, LAI, coef, omega = vegetation
    f, theta = SAR

    ke = coef*np.sqrt(LAI)

    soil = Soil(mv=sm, s=s, clay=clay, sand=sand, f=f, bulk=bulk)

    can = OneLayer(canopy='turbid_isotropic', ke_h=ke, ke_v=ke, d=d, ks_h = omega*ke, ks_v = omega*ke)

    S = model.RTModel(surface=soil, canopy=can, models= {'surface': 'Oh92', 'canopy': 'turbid_isotropic'}, theta=theta, freq=f)
    S.sigma0()

    vv = S.__dict__['stot']['vv']
    vh = S.__dict__['stot']['hv']

    return vv, vh
