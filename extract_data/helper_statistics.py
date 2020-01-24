
import numpy as np

def _error(actual: np.ndarray, predicted: np.ndarray):
    """ Simple error """
    return actual - predicted

def me(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Error """
    return np.nanmean(_error(actual, predicted))

def mse(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Squared Error """
    return np.nanmean(np.square(_error(actual, predicted)))

def rmse(actual: np.ndarray, predicted: np.ndarray):
    """ Root Mean Squared Error """
    return np.sqrt(mse(actual, predicted))

def ubrmse(actual: np.ndarray, predicted: np.ndarray):
    """ unbiased Root Mean Squared Error """
    return np.sqrt(np.nansum((actual-(predicted-np.nanmean(actual)))**2)/len(actual))

def linregress(predictions, targets):
    """ Calculate a linear least-squares regression for two sets of measurements """
    slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(predictions, targets)
    return slope, intercept, r_value, p_value, std_err
