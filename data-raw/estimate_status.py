# estimating lidar measurement status with HMRF

import xml.etree.ElementTree
import numpy as np
import pandas as pd
import xarray as xr
import sqlalchemy
from rasppy.hmrf import make_segmentation, LidarSamples
pg = sqlalchemy.create_engine('postgresql:///lidar')

scan = 188


def pg_to_xr(pg, scan_id, start_time):
    '''
    Create an xarray Dataset from postgres lidar data
    '''
    var1d = ['los_id']
    columns = ['los_id', 'rws', 'cnr', 'status']
    # get the lidar data
    q = "select time, los_id, rws, cnr, status from profiles where scan_id=%s and time between '%s' and timestamp '%s' + interval '1 day' order by time"
    q1 = q % (scan_id, start_time, start_time)
    d1 = pd.read_sql(q1, pg)
    ntimes = d1.shape[0]

    # get the scan data
    query = ('select xml from scans where id=' + str(scan_id))
    scan_xml = pd.read_sql(query, pg)['xml'][0]
    scan = xml.etree.ElementTree.fromstring(scan_xml).find('.//scan')
    if 'display_resolution_m' in scan.attrib.keys():
        # if the scan has a regular resolution
        range_min = float(scan.attrib['minimum_range_m'])
        range_res = float(scan.attrib['display_resolution_m'])
        ngates = int(scan.attrib['number_of_gates'])
        range_coords = range_min + np.array(range(ngates)) * range_res
    else:
        range_coords = np.array(list(map(float, scan.attrib['distances_m'].split(', '))))
        # if the scan has oddly chosen numbers

    # construct the dataset
    xr_dims = {'Time': ntimes, 'Range': range_coords.shape[0]}
    time_coords = d1['time'].values
    xr_coords = {'Time': time_coords, 'Range': range_coords}
    xr_vars = {}
    for var in columns:
        if var in var1d:
            xr_coords[var] = ('Time', d1[var].values.tolist())
        else:
            xr_vars[var] = (['Time', 'Range'],
                            np.array(d1[var].values.tolist()))
    xr_attrs = {}
    if 'elevation_angle_deg' in scan.attrib.keys():
        xr_attrs['scan_elevation_angle_deg'] = scan.attrib['elevation_angle_deg']
    x1 = xr.Dataset(data_vars=xr_vars, coords=xr_coords, attrs=xr_attrs)
    rename_dict = {'los_id': 'LOS', 'rws': 'RWS', 'cnr': 'CNR',
                   'status': 'Status'}
    return x1.rename(rename_dict)


def make_segmentations(lidar, **seg_args):
    # make segmentation objects for a lidar in DBS mode
    lidar2 = lidar.rasp.los_format()
    samples = []
    for i in range(lidar2.dims['LOS']):
        rws = lidar2['RWS'].sel(LOS=i).swap_dims({'scan': 'Time'})
        cnr = lidar2['CNR'].sel(LOS=i).swap_dims({'scan': 'Time'})
        samples.append(make_segmentation(rws, cnr, **seg_args))
        
    return samples


def estimate_status(lidar, freeze=(), **seg_args):
    samples = make_segmentations(lidar, beta=.4)
    ls = LidarSamples(samples)
    ls.run(10, freeze=freeze)
    
    lidar2 = lidar.rasp.los_format()
    lidar2['status'] = (lidar2['RWS'].dims, np.empty(lidar2['RWS'].shape))
    for i in range(lidar2.dims['LOS']):
        lidar0 = lidar2.sel(LOS=i).swap_dims({'scan': 'Time'})
        da = xr.concat([lidar0['RWS'], lidar0['CNR']], 'series').transpose('Time', 'Range', 'series')
        scan_is_complete = ~np.isnan(da).any(['Range', 'series'])
        lidar2['status'][:,scan_is_complete,i] = ls.samples[i].ppm[:,:,0].transpose()
        lidar2['status'][:,~scan_is_complete,i] = np.NaN
        
    lidar2['status'].attrs['mu'] = ls.mu
    lidar2['status'].attrs['sigma'] = ls.sigma
        
    return lidar2['status'].stack(profile=['scan', 'LOS']).swap_dims({'profile': 'Time'}).drop('profile')
        


# get applicable days and hmrf parameters
dq = "select distinct time_bucket('1 day', time) as date from profiles where scan_id=%s" % (scan, )
days_df = pd.read_sql(dq, pg)
pq = "select scan_id, mu::numeric[] as mu, sigma::numeric[] as sigma from hmrf_params where scan_id=%s" % (scan, )
params_df = pd.read_sql(pq, pg)
mu = params_df['mu'][0]
sigma = params_df['sigma'][0]

# for each day of lidar data, run the hmrf analysis
for r in days_df.itertuples():
    d = r.date
    print(d)
    ds = pg_to_xr(pg, scan, d)
    ds['hmrf'] = estimate_status(ds, freeze=[0, 1],
                                 mu=mu, sigma=sigma)

    # save the status (round to 6 decimals to match postgres real
    # type)
    hmrf_df = pd.DataFrame({'scan_id': scan,
                            'time': ds.coords['Time'],
                            'hmrf': ds['hmrf'].round(6).transpose().values.tolist()})

    # save the status estimates
    hmrf_df.to_sql('status', pg, if_exists='append', index=False)

    # save the wind estimates
    ds['hmrf_status'] = ds['hmrf'] > .5
    w = ds.rasp.estimate_wind(filter='hmrf_status')
    w5m = w.resample(Time='5T').mean('Time').round(6)
    xwind = w5m.sel(Component='x').values.tolist()
    ywind = w5m.sel(Component='y').values.tolist()
    zwind = w5m.sel(Component='z').values.tolist()
    w5m_df = pd.DataFrame({'scan_id': scan,
                           'time': w5m.coords['Time'],
                           'xwind': xwind,
                           'ywind': ywind,
                           'zwind': zwind})
    w5m_df.to_sql('hmrf5m', pg, if_exists='append', index=False)
