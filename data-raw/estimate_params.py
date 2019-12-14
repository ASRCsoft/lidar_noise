# estimating HMRF parameters using samples of lidar data from the
# database

import xml.etree.ElementTree
import numpy as np
import pandas as pd
import xarray as xr
import sqlalchemy
from rasppy.hmrf import make_segmentation, LidarSamples
pg = sqlalchemy.create_engine('postgresql:///lidar')

# the scan IDs to make estimates for
# scan_ids = [66]


# turn this into xarray
def pg_to_xr(pg, scan_id, los, start_time):
    '''
    Create an xarray Dataset from postgres lidar data
    '''
    var1d = ['los_id']
    columns = ['los_id', 'rws', 'cnr', 'status']
    # get the lidar data
    q = "select time, los_id, rws, cnr, status from profiles where scan_id=%s and los_id=%s and time between '%s' and timestamp '%s' + interval '3 hours' order by time"
    q1 = q % (scan_id, los, start_time, start_time)
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
    return x1


# the plan:

# 1) get random samples from postgres

# choose 50 random samples
# scan_ids_str = ','.join(map(str, scan_ids))
scan_query_str = "select id from scans where lidar_id in (select id from lidars where site='CESTM_roof') and (xpath('//scan/@mode', xml)::varchar[])[1]='dbs' and (xpath('//scan/@minimum_range_m', xml)::varchar[])[1]::int=100 and (xpath('//scan/@maximum_range_m', xml)::varchar[])[1]::int=3000 and (xpath('//scan/@display_resolution_m', xml)::varchar[])[1]::int=25 and not xpath_exists('//scan[@distances_m]', xml)"
q = "select * from (select distinct scan_id, time_bucket('3 hours', time) as time, los_id, row_number() over (partition by scan_id order by random() desc) as row_id from profiles where scan_id in (%s)) s1 where row_id<=50" % (scan_query_str)
sdf = pd.read_sql(q, pg)
# make this easy for later-- write to a file
sdf.to_csv('random_samples.csv')
sdf = pd.read_csv('random_samples.csv')


# 2) estimate parameters using rasppy
scan_ids = np.unique(sdf['scan_id'])
mus = []
sigmas = []
for sid in scan_ids:
    print('starting scan %s...' % (sid))
    sdf0 = sdf.loc[sdf['scan_id'] == sid]
    # get the 50 samples
    samples = []
    dss = []
    for row in sdf0.itertuples():
        scan_id = row.scan_id
        los = row.los_id
        start_time = row.time
        ds = pg_to_xr(pg, scan_id, los, start_time)
        dss.append(ds)
        samples.append(make_segmentation(ds['rws'].transpose(),
                                         ds['cnr'].transpose()))
        
    

    ls = LidarSamples(samples)
    ls.run(10)
    mus.append(ls.mu.tolist())
    # array([[ -5.02366719e-03,   1.82511795e-01],
    #        [  0.00000000e+00,  -3.19116728e+01]])
    sigmas.append(ls.sigma.tolist())
    # array([[[  2.25486004e-01,   5.85098219e-03],
    #         [  5.85098219e-03,   2.67645149e+00]],
    #        [[  3.00000000e+02,   0.00000000e+00],
    #         [  0.00000000e+00,   4.40290152e+00]]])

    # print out some cool stuff
    old_count = sum(map(lambda x: x['status'].sum(), dss)).values
    new_count = sum(map(lambda x: np.sum(x.ppm[:,:,0] > .5), ls.samples))
    print('Leosphere: %s, HMRF: %s' % (old_count, new_count))
    
    
# 3) store the estimated parameters in postgres
    
# the results data frame
rdf = pd.DataFrame({'scan_id': scan_ids,
                    'mu': mus,
                    'sigma': sigmas})
rdf.to_sql('hmrf_params', pg, if_exists='replace', index=False)



# take a look
import matplotlib.pyplot as plt
i = 7
ds = dss[i]
seg = ls.samples[i]
# ds['rws'].plot(x='Time', y='Range')
# plt.show()
# ds['rws'].where(ds['status']).plot(x='Time', y='Range')
# plt.show()
# ds['rws'].where(seg.ppm[:,:,0] > .5).plot(x='Time', y='Range')
# plt.show()

rwss = xr.concat([ds['rws'],
                  ds['rws'].where(ds['status']),
                  ds['rws'].where(seg.ppm[:,:,0] > .5)],
                 dim='Filter')
rwss.coords['Filter'] = ['None', 'CNR Threshold', 'HMRF']

rwss.plot(x='Time', y='Range', col='Filter')
plt.show()


sum(map(lambda x: x['status'].sum(), dss))
sum(map(lambda x: np.sum(x.ppm[:,:,0] > .5), ls.samples))
