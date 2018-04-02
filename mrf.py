# functions for solving markov random field models

import math
import numpy as np
import xarray as xr
from scipy.stats import uniform, norm
from scipy.ndimage.filters import median_filter, uniform_filter, gaussian_filter


def prepare_dataset(cnr, rws, filter_size=(7, 59)):
    # create the dataset
    lidar = xr.Dataset({'CNR': cnr, 'RWS': rws})

    # add states
    states = ['wind', 'noise']
    n_states = len(states)
    # q is the set of state marginal probabilities for each pixel
    q_dims = lidar['RWS'].dims + ('State', )
    q_shape = lidar['RWS'].shape + (n_states, )
    lidar['q'] = (lidar['RWS'].dims + ('State', ), np.full(q_shape, 1 / n_states))
    lidar.coords['State'] = states

    # U is a Potts model interaction Hamilton matrix
    lidar['U'] = (('State', 'Neighbor State'), np.ones((lidar.dims['State'], ) * 2))
    lidar.coords['Neighbor State'] = lidar.coords['State'].values
    lidar['U'].values[np.diag_indices_from(lidar['U'])] = 0

    # the (initial) state parameter values for autoregressive CNR model
    lidar['mu'] = (('State', 'Series'), [[0, 0], [0, -31.5]])
    lidar['sigma'] = (('State', 'Series'), [[1, 1], [300, 2]])
    # assignment of floats won't work if it's stored as an array of ints!
    lidar['sigma'] = lidar['sigma'].astype(float)
    lidar.coords['Series'] = ['RWS', 'CNR']

    lidar2 = lidar.rasp.los_format()

    # filtered RWS values
    lidar2['filtered_RWS'] = (lidar2['RWS'].dims, np.empty(lidar2['RWS'].shape))
    lidar2['filtered_CNR'] = (lidar2['CNR'].dims, np.empty(lidar2['CNR'].shape))
    for i in range(lidar2.dims['LOS']):
        lidar2['filtered_RWS'][:,:,i] = median_filter(lidar2['RWS'].sel(LOS=i), size=filter_size)
        #lidar2['filtered_CNR'][:,:,i] = median_filter(lidar2['CNR'].sel(LOS=i), size=(7, 59))
        scan_not_nan = ~np.isnan(lidar2['CNR'].sel(LOS=i)).any('Range')
        lidar2['filtered_CNR'][:,scan_not_nan,i] = uniform_filter(lidar2['CNR'].sel(LOS=i, scan=scan_not_nan), size=filter_size)
        lidar2['filtered_CNR'][:,~scan_not_nan,i] = np.NaN
    
    return lidar2

def ve_step1(lidar, beta=.1):
    # VE step
    # get the sum of the 8 neighborhood state probabilities (since U is the same for all points we can do this)
    Nq = xr.concat([lidar['q'].shift(scan=1),
                    lidar['q'].shift(scan=1, Range=-1),
                    lidar['q'].shift(Range=-1),
                    lidar['q'].shift(scan=-1, Range=-1),
                    lidar['q'].shift(scan=-1),
                    lidar['q'].shift(scan=-1, Range=1),
                    lidar['q'].shift(Range=1),
                    lidar['q'].shift(scan=1, Range=1)], dim='neighbor').sum('neighbor')
    Nq = Nq.rename({'State': 'Neighbor State'})
    # probability of RWS measurements given wind
    p_wind = norm.pdf(lidar['RWS'], lidar['filtered_RWS'], lidar['sigma'].sel(Series='RWS', State='wind'))
    # probability of CNR measurements given wind
    p_wind *= norm.pdf(lidar['CNR'], lidar['filtered_CNR'], lidar['sigma'].sel(Series='CNR', State='wind'))
    # probability of RWS measurements given noise
    p_noise = uniform.pdf(lidar['RWS'], lidar['RWS'].min(), lidar['RWS'].max() - lidar['RWS'].min())
    # probability of CNR measurements given noise
    p_noise *= (norm.pdf(lidar['CNR'], lidar['mu'].sel(Series='CNR', State='noise'),
                         lidar['sigma'].sel(Series='CNR', State='noise')))
    p_wind = xr.DataArray(p_wind, dims=lidar['RWS'].dims)
    p_noise = xr.DataArray(p_noise, dims=lidar['RWS'].dims)
    q2 = xr.concat([p_wind, p_noise], dim='State')
    # add the U energy
    q2 = (q2 * np.exp(-2 * beta * (Nq * lidar['U']).sum('Neighbor State')))
    return q2 / q2.sum('State')

def vm_step1(lidar):
    # VM step
    # mean noise CNR
    mu = lidar['mu'].copy()
    mu[1,1] = (lidar['q'].sel(State='noise') * lidar['CNR']).sum() / lidar['q'].sel(State='noise').sum()

    # wind RWS variance
    sigma = lidar['sigma'].copy()
    sigma[0,0] = math.sqrt((lidar['q'].sel(State='wind') * (lidar['RWS'] - lidar['filtered_RWS'])**2).sum() /
                           lidar['q'].sel(State='wind').sum())
    # wind CNR variance
    sigma[0,1] = math.sqrt((lidar['q'].sel(State='wind') * (lidar['CNR'] - lidar['filtered_CNR'])**2).sum() /
                           lidar['q'].sel(State='wind').sum())
    # noise CNR variance
    sigma[1,1] = math.sqrt((lidar['q'].sel(State='noise') * (lidar['CNR'] - lidar['mu'].sel(Series='CNR', State='noise'))**2).sum() /
                           lidar['q'].sel(State='noise').sum())
    return mu, sigma    

def vem1(lidar, steps=10, beta=.1):
    for i in range(steps):
        # VE step
        lidar['q'] = ve_step1(lidar, beta)
        
        # VM step
        mu, sigma = vm_step1(lidar)
        lidar['mu'] = mu
        lidar['sigma'] = sigma
    return lidar['q']

def estimate_status(lidar, steps=10, beta=.1, filter_size=(7, 59)):
    mrf_ds = prepare_dataset(lidar['CNR'], lidar['RWS'], filter_size)
    # get the probability of the data being good
    new_status = vem1(mrf_ds, steps, beta).sel(State='wind').drop('State')
    # put it back into the original format
    new_status = new_status.stack(profile=('scan', 'LOS')).swap_dims({'profile': 'Time'}).drop('profile')
    # drop 'not a time' profiles (that were added when we switched to LOS format)
    new_status = new_status.sel(Time=~np.isnat(new_status.coords['Time']))
    return new_status

# def vem2(lidar2, steps=1, beta=.1):
#     for i in range(steps):
#         # VE step
#         # get the sum of the 8 neighborhood state probabilities (since U is the same for all points we can do this)
#         lidar2['Nq'] = xr.concat([lidar2['q'].shift(scan=1),
#                                   lidar2['q'].shift(scan=1, Range=-1),
#                                   lidar2['q'].shift(Range=-1),
#                                   lidar2['q'].shift(scan=-1, Range=-1),
#                                   lidar2['q'].shift(scan=-1),
#                                   lidar2['q'].shift(scan=-1, Range=1),
#                                   lidar2['q'].shift(Range=1),
#                                   lidar2['q'].shift(scan=1, Range=1)], dim='neighbor').sum('neighbor')
#         # probability of RWS measurements given wind
#         p_wind = (norm.pdf(lidar2['RWS'], lidar2['filtered_RWS'], lidar2['sigma'].sel(Series='RWS', State='wind')) *
#                   np.exp(-2 * beta * (lidar2['Nq'] * lidar2['U'][:,0]).sum('State')))
#         # probability of CNR measurements given wind
#         p_wind *= (norm.pdf(lidar2['CNR'], lidar2['mu'].sel(Series='CNR', State='wind'),
#                             lidar2['sigma'].sel(Series='CNR', State='wind')) *
#                    np.exp(-2 * beta * (lidar2['Nq'] * lidar2['U'][:,0]).sum('State')))
#         # probability of RWS measurements given noise
#         p_noise = (uniform.pdf(lidar2['RWS'], lidar2['RWS'].min(), lidar2['RWS'].max() - lidar['RWS'].min()) *
#                    np.exp(-2 * beta * (lidar2['Nq'] * lidar2['U'][:,1]).sum('State')))
#         # probability of CNR measurements given noise
#         p_noise *= (norm.pdf(lidar2['CNR'], lidar2['mu'].sel(Series='CNR', State='noise'),
#                              lidar2['sigma'].sel(Series='CNR', State='noise')) *
#                     np.exp(-2 * beta * (lidar2['Nq'] * lidar2['U'][:,1]).sum('State')))
#         lidar2['q'] = xr.concat([p_wind, p_noise], dim='State') / (p_wind + p_noise)
        
#         # VM step
#         # mean wind CNR
#         lidar2['mu'][0,1] = (lidar2['q'].sel(State='wind') * lidar2['CNR']).sum() / lidar2['q'].sel(State='wind').sum()

#         # mean noise CNR
#         lidar2['mu'][1,1] = (lidar2['q'].sel(State='noise') * lidar2['CNR']).sum() / lidar2['q'].sel(State='noise').sum()

#         # wind RWS variance
#         lidar2['sigma'][0,0] = math.sqrt((lidar2['q'].sel(State='wind') * (lidar2['RWS'] - lidar2['filtered_RWS'])**2).sum() /
#                                          lidar2['q'].sel(State='wind').sum())
#         # wind CNR variance
#         lidar2['sigma'][0,1] = math.sqrt((lidar2['q'].sel(State='wind') * (lidar2['CNR'] - lidar2['mu'].sel(Series='CNR', State='wind'))**2).sum() /
#                                          lidar2['q'].sel(State='wind').sum())
#         # noise CNR variance
#         lidar2['sigma'][1,1] = math.sqrt((lidar2['q'].sel(State='noise') * (lidar2['CNR'] - lidar2['mu'].sel(Series='CNR', State='noise'))**2).sum() /
#                                          lidar2['q'].sel(State='noise').sum())
#     return lidar2['q']
