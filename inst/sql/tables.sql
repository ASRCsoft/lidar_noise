-- setting up the database and tables for the lidar database

create extension if not exists timescaledb cascade;

-- lidar related data:
create table sites (
  stid varchar primary key,
  number smallint,
  name varchar,
  latitude float,
  longitude float,
  elevation float,
  county varchar,
  nearest_city varchar,
  state varchar,
  distance_from_town float,
  direction_from_town varchar,
  climate_division smallint,
  climate_division_name varchar,
  wfo varchar,
  commissioned timestamp,
  decommissioned timestamp
);
-- add data from the csv file?
-- copy sites from '/home/xcite/lidar_db/NYSM_Profiler_Locations_20170920.csv' delimiter ',' csv header;

-- 'name' is the displayed name, stid is 'CESTM_roof-14' etc.
create table lidars (
  id smallserial primary key,
  stid varchar unique not null,
  site varchar references sites,
  name varchar
);

create table scans (
  id smallserial primary key,
  lidar_id smallint references lidars not null,
  xml xml not null
);

create table lidar_configs (
  id smallserial primary key,
  lidar_id smallint references lidars not null,
  xml xml not null
);

-- To cut down on the number of rows in the data tables, I'm stuffing
-- all the data associated with each profile into one row. That means
-- ~100 radial wind speed measurements, ~100 CNR measurements, etc. in
-- every row. To do this I'm storing each set of measurements as an
-- array. This cuts down the number of rows that have to be found by a
-- factor of ~100, which makes it much more practical to access the
-- data.

-- Even after cutting it down by a factor of 100, we still have an
-- impractically large number of rows. To help deal with that I'm
-- using timescaledb: https://www.timescale.com/

-- Updating poses another problem. When running update queries, behind
-- the scenes postgres deletes the affected rows and replaces them
-- with totally new rows. That means if I want to update one value in
-- a row all the other values in that row will also be discarded and
-- replaced. Thus to make updates faster it's better to split the data
-- into logical groupings of variables that will typically be updated
-- in common, rather than having all data combined in one common
-- table.

-- profiles (lidar data)
create table profiles (
  configuration_id smallint,
  scan_id smallint not null references scans,
  sequence_id int,
  los_id smallint,
  azimuth real,
  elevation real,
  time timestamp not null,
  cnr real[],
  rws real[],
  drws real[],
  status boolean[],
  error real[],
  confidence real[],
  primary key(scan_id, time)
);
select create_hypertable('profiles', 'time', chunk_time_interval => interval '1 day');


-- wind stuff-- what do I do with this?

-- -- really this could be added to profiles but that would mean every
-- -- time I want to update wind values I have to update the entire
-- -- profiles table (horrifying!). So instead it gets its own smaller
-- -- table that's easier to update
-- create table wind (lidar_id smallint not null, scan_id smallint not null, time timestamp not null, xwind real[], ywind real[], zwind real[], primary key(lidar_id, time), foreign key(lidar_id, scan_id) references scans(lidar_id, id));
-- -- make it a hypertable:
-- select create_hypertable('wind', 'time', chunk_time_interval => '1 day');

-- -- lidar 5 minute summary data (the default timescaledb interval of 1 month should be fine here)
-- -- this is used for the 'quick look' tool (profiles page) on the xcite website
-- create table lidar5m (scan_id smallint not null, time timestamp not null, cnr real[], cnr_whole real[], drws real[], xwind real[], ywind real[], zwind real[], primary key(scan_id, time), foreign key(scan_id) references scans);
-- select create_hypertable('lidar5m', 'time');

-- -- lidar 15 minute summary data
-- -- this is for the CNR gradient for PBL calculations
-- create table lidar15m (scan_id smallint not null, time timestamp not null, cnr_whole real[], zwind_var real[], zwind_n int[], primary key(scan_id, time), foreign key(scan_id) references scans);
-- select create_hypertable('lidar15m', 'time');

-- -- lidar 30 minute summary data
-- -- also need vertical wind variance for PBL calculations

-- -- add z wind
-- create table lidar30m (scan_id smallint not null, time timestamp not null, zwind_var real[], n int[], primary key(scan_id, time), foreign key(scan_id) references scans);
-- select create_hypertable('lidar30m', 'time');
-- -- this was for a non-existent wind page (the tke calculations were incorrect)
-- -- create table lidar30m (scan_id smallint not null, time timestamp not null, tke real[], alpha real[], primary key(scan_id, time), foreign key(scan_id) references scans);


-- radiosonde data
create schema radiosonde;
create table radiosonde.releases (
  id serial primary key,
  file text,
  version text,
  station text,
  flight text,
  time timestamp
);

create table radiosonde.records (
  release_id int references radiosonde.releases,
  elapsed_time float,
  time_stamp timestamp,
  corrected_pressure float,
  smoothed_pressure float,
  geopotential_height int,
  corrected_temperature float,
  potential_temperature float,
  corrected_rh float,
  dewpoint_temperature float,
  dewpoint_depression float,
  mixing_ratio float,
  ascension_rate float,
  temperature_lapse float,
  corrected_azimuth float,
  corrected_elevation float,
  wind_direction float,
  wind_speed float,
  u float,
  v float,
  latitude float,
  longitude float,
  geometric_height float,
  arc_distance float,
  primary key(release_id, time_stamp)
);
select create_hypertable('radiosonde.records', 'time_stamp');
