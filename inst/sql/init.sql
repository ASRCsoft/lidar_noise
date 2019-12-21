-- setting up the database tables

-- Lidar related data
create table lidar_sites (
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
  commissioned text,
  decommissioned text
);
-- add data from the csv file?
-- copy sites from '/home/xcite/lidar_db/NYSM_Profiler_Locations_20170920.csv' delimiter ',' csv header;

create table lidars (
  id integer primary key,
  stid varchar references lidar_sites not null,
  name varchar unique not null
);

-- lidar scanning modes
create table scans (
  id integer primary key,
  lidar_id smallint references lidars not null,
  xml text not null
);

-- date ranges of scan data availability
create table scan_dates (
  scan_id smallint references scans not null,
  start_date text not null,
  end_date text not null
);


-- Intermediate results data
-- ?


-- -- Radiosonde data?
-- create schema radiosonde;
-- create table radiosonde.releases (
--   id serial primary key,
--   file text,
--   version text,
--   station text,
--   flight text,
--   time timestamp
-- );

-- create table radiosonde.records (
--   release_id int references radiosonde.releases,
--   elapsed_time float,
--   time_stamp timestamp,
--   corrected_pressure float,
--   smoothed_pressure float,
--   geopotential_height int,
--   corrected_temperature float,
--   potential_temperature float,
--   corrected_rh float,
--   dewpoint_temperature float,
--   dewpoint_depression float,
--   mixing_ratio float,
--   ascension_rate float,
--   temperature_lapse float,
--   corrected_azimuth float,
--   corrected_elevation float,
--   wind_direction float,
--   wind_speed float,
--   u float,
--   v float,
--   latitude float,
--   longitude float,
--   geometric_height float,
--   arc_distance float,
--   primary key(release_id, time_stamp)
-- );
