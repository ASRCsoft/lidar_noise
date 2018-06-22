'''Get paper data from postgres'''

import sqlalchemy
import xarray as xr

# set up database connection
with open('database.txt') as f:
    dbconfig = f.readlines()[0].strip()
pg = sqlalchemy.create_engine(dbconfig)

# get data
# ...

# write to netcdf file
# ...
