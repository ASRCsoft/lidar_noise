-- using radiosonde data to validate wind estimates

--------------------------------------------------
-- need to convert radiosonde heights to lidar heights
-- can do that by subtracting lidar elevation from radiosonde Geometric Height?

-- now that heights are converted need to place records into the correct bins
-- can do with integer division
--------------------------------------------------
-- remove data where status==0
CREATE 
OR 
REPLACE FUNCTION height_to_range(h double precision)
returns int AS $$
select ((h - 103.7) / 25)::int - 3;
$$ language sql;

-- ^^ would be really cool to do with a function that takes scanning
-- mode as an argument

-- see if scanning mode matches specification
CREATE 
OR 
REPLACE FUNCTION is_100_to_3000_by_25(xml xml)
returns boolean AS $$
select (xpath('//scan/@mode', xml)::text[])[1]='dbs'
       and (xpath('//scan/@display_resolution_m', xml)::text[])[1]::int=25
       and (xpath('//scan/@elevation_angle_deg', xml)::text[])[1]::int=75
       and (xpath('//scan/@minimum_range_m', xml)::text[])[1]::int=100
       and (xpath('//scan/@number_of_gates', xml)::text[])[1]::int=117;
$$ language sql;



-- statistical summaries



-- useful views for comparing wind estimates
create materialized view leosphere_comparison as
       select lidar.scan_id,
       	      lidar.time,
       	      lidar.range,
	      lidar.u as lidar_u,
	      lidar.v as lidar_v,
	      sonde.u as sonde_u,
	      sonde.v as sonde_v
       from (select time_bucket('5 minutes', time_stamp) as time,
             	    height_to_range(geometric_height) as range,
             	    u, v
      	     from radiosonde.records
             where geometric_height - 103.7>87.5
	     and u is not null
	     and v is not null
	     -- and wind_speed is not null
	     -- and wind_direction is not null
            ) as sonde
            join
            (select scan_id,
	    	    time,
                    generate_subscripts(xwind, 1) as range,
                    unnest(xwind) as u,
                    unnest(ywind) as v
             from lidar5m
             where scan_id in (select distinct scan_id
	     	   	       from hmrf5m
			      )
            ) as lidar
            on sonde.time=lidar.time
            and sonde.range=lidar.range
       where lidar.u!=real 'NaN' and lidar.v!=real 'NaN';

create materialized view hmrf_comparison as
       select lidar.scan_id,
       	      lidar.time,
       	      lidar.range,
	      lidar.u as lidar_u,
	      lidar.v as lidar_v,
	      sonde.u as sonde_u,
	      sonde.v as sonde_v
       from (select time_bucket('5 minutes', time_stamp) as time,
             	    height_to_range(geometric_height) as range,
             	    u, v
      	     from radiosonde.records
             where geometric_height - 103.7>87.5
	     and u is not null
	     and v is not null
	     -- and wind_speed is not null
	     -- and wind_direction is not null
            ) as sonde
            join
            (select scan_id,
	    	    time,
                    generate_subscripts(xwind, 1) as range,
                    unnest(xwind) as u,
                    unnest(ywind) as v
             from hmrf5m
            ) as lidar
            on sonde.time=lidar.time
            and sonde.range=lidar.range
       where lidar.u!=real 'NaN' and lidar.v!=real 'NaN';



-- U and V comparisons
select range,
       avg(abs(lidar_u - sonde_u))::real as mae_u,
       avg(abs(lidar_v - sonde_v))::real as mae_v,
       avg((lidar_u - sonde_u)^2)::real as mse_u,
       avg((lidar_v - sonde_v)^2)::real as mse_v,
       corr(lidar_u, sonde_u)::real as corr_u,
       corr(lidar_v, sonde_v)::real as corr_v,
       count(*)
from leosphere_comparison
group by range
order by range asc;

select range,
       avg(abs(lidar_u - sonde_u))::real as mae_u,
       avg(abs(lidar_v - sonde_v))::real as mae_v,
       avg((lidar_u - sonde_u)^2)::real as mse_u,
       avg((lidar_v - sonde_v)^2)::real as mse_v,
       corr(lidar_u, sonde_u)::real as corr_u,
       corr(lidar_v, sonde_v)::real as corr_v,
       count(*)
from hmrf_comparison
group by range
order by range asc;

-- writing files
\copy (select range,
       avg(abs(lidar_u - sonde_u))::real as mae_u,
       avg(abs(lidar_v - sonde_v))::real as mae_v,
       avg((lidar_u - sonde_u)^2)::real as mse_u,
       avg((lidar_v - sonde_v)^2)::real as mse_v,
       corr(lidar_u, sonde_u)::real as corr_u,
       corr(lidar_v, sonde_v)::real as corr_v,
       count(*)
from leosphere_comparison
group by range
order by range asc) to 'cnr_stats_combined.csv' delimiter ',' csv header

\copy (select range,
       avg(abs(lidar_u - sonde_u))::real as mae_u,
       avg(abs(lidar_v - sonde_v))::real as mae_v,
       avg((lidar_u - sonde_u)^2)::real as mse_u,
       avg((lidar_v - sonde_v)^2)::real as mse_v,
       corr(lidar_u, sonde_u)::real as corr_u,
       corr(lidar_v, sonde_v)::real as corr_v,
       count(*)
from hmrf_comparison
group by range
order by range asc) to 'hmrf_stats_combined.csv' delimiter ',' csv header

\copy (select scan_id,
       range,
       avg(abs(lidar_u - sonde_u))::real as mae_u,
       avg(abs(lidar_v - sonde_v))::real as mae_v,
       avg((lidar_u - sonde_u)^2)::real as mse_u,
       avg((lidar_v - sonde_v)^2)::real as mse_v,
       corr(lidar_u, sonde_u)::real as corr_u,
       corr(lidar_v, sonde_v)::real as corr_v,
       count(*)
from leosphere_comparison
group by scan_id, range
order by scan_id, range asc) to 'cnr_stats_separate.csv' delimiter ',' csv header

\copy (select scan_id,
       range,
       avg(abs(lidar_u - sonde_u))::real as mae_u,
       avg(abs(lidar_v - sonde_v))::real as mae_v,
       avg((lidar_u - sonde_u)^2)::real as mse_u,
       avg((lidar_v - sonde_v)^2)::real as mse_v,
       corr(lidar_u, sonde_u)::real as corr_u,
       corr(lidar_v, sonde_v)::real as corr_v,
       count(*)
from hmrf_comparison
group by scan_id, range
order by scan_id, range asc) to 'hmrf_stats_separate.csv' delimiter ',' csv header


-- U and V stats (hmrf)
\copy (select lidar.range,
       -- lidar.u, sonde.u, lidar.v, sonde.v
       avg(abs(lidar.u - sonde.u))::real as mae_u,
       avg(abs(lidar.v - sonde.v))::real as mae_v,
       avg((lidar.u - sonde.u)^2)::real as mse_u,
       avg((lidar.v - sonde.v)^2)::real as mse_v,
       corr(lidar.u, sonde.u)::real as corr_u,
       corr(lidar.v, sonde.v)::real as corr_v,
       count(*)
from (select time_bucket('5 minutes', time_stamp) as time,
             height_to_range(geometric_height) as range,
             u, v
      from radiosonde.records
      where geometric_height - 103.7>87.5
	    and u is not null
	    and v is not null
	    -- and wind_speed is not null
	    -- and wind_direction is not null
     ) as sonde
     join
     (select time,
             generate_subscripts(xwind, 1) as range,
             unnest(xwind) as u,
             unnest(ywind) as v
      from hmrf5m
      -- where scan_id in (select id
      --                   from scans
      -- 		        where is_100_to_3000_by_25(xml))
      where scan_id=66
     ) as lidar
     on sonde.time=lidar.time
        and sonde.range=lidar.range
where lidar.u!=real 'NaN' and lidar.v!=real 'NaN'
-- limit 20;
group by lidar.range
order by lidar.range asc) to 'hmrf_stats.csv' delimiter ',' csv header


-- U and V stats (Leosphere)
\copy (select lidar.range,
       -- lidar.u, sonde.u, lidar.v, sonde.v
       avg(abs(lidar.u - sonde.u))::real as mae_u,
       avg(abs(lidar.v - sonde.v))::real as mae_v,
       avg((lidar.u - sonde.u)^2)::real as mse_u,
       avg((lidar.v - sonde.v)^2)::real as mse_v,
       corr(lidar.u, sonde.u)::real as corr_u,
       corr(lidar.v, sonde.v)::real as corr_v,
       count(*)
from (select time_bucket('5 minutes', time_stamp) as time,
             height_to_range(geometric_height) as range,
             u, v
      from radiosonde.records
      where geometric_height - 103.7>87.5
	    and u is not null
	    and v is not null
	    -- and wind_speed is not null
	    -- and wind_direction is not null
     ) as sonde
     join
     (select time,
             generate_subscripts(xwind, 1) as range,
             unnest(xwind) as u,
             unnest(ywind) as v
      from lidar5m
      -- where scan_id in (select id
      --                   from scans
      -- 		   where is_100_to_3000_by_25(xml))
      where scan_id=66
     ) as lidar
     on sonde.time=lidar.time
        and sonde.range=lidar.range
where lidar.u!=real 'NaN' and lidar.v!=real 'NaN'
-- limit 20;
group by lidar.range
order by lidar.range asc) to 'cnr_stats.csv' delimiter ',' csv header


select lidar.range,
       -- lidar.u, sonde.u, lidar.v, sonde.v
       avg(abs(lidar.u - sonde.u))::real as mae_u,
       avg(abs(lidar.v - sonde.v))::real as mae_v,
       avg((lidar.u - sonde.u)^2)::real as mse_u,
       avg((lidar.v - sonde.v)^2)::real as mse_v,
       corr(lidar.u, sonde.u)::real as corr_u,
       corr(lidar.v, sonde.v)::real as corr_v,
       count(*)
from (select time_bucket('5 minutes', time_stamp) as time,
             height_to_range(geometric_height) as range,
             u, v
      from radiosonde.records
      where geometric_height - 103.7>87.5
	    and u is not null
	    and v is not null
	    -- and wind_speed is not null
	    -- and wind_direction is not null
     ) as sonde
     join
     (select time,
             generate_subscripts(xwind, 1) as range,
             unnest(xwind) as u,
             unnest(ywind) as v
      from hmrf5m
      -- where scan_id in (select id
      --                   from scans
      -- 		        where is_100_to_3000_by_25(xml))
      where scan_id=188
     ) as lidar
     on sonde.time=lidar.time
        and sonde.range=lidar.range
where lidar.u!=real 'NaN' and lidar.v!=real 'NaN'
-- limit 20;
group by lidar.range
order by lidar.range asc;





-- comparisons between methods

-- count the number of measurements in a single profile
CREATE 
OR 
REPLACE FUNCTION n_values(a real[])
returns bigint AS $$
select (SELECT SUM((s!='NaN')::int) FROM UNNEST(a) s);
$$ language sql;

-- count the number of measurements in a scanning mode
CREATE 
OR 
REPLACE FUNCTION n_leosphere(id int)
returns numeric AS $$
select sum(n_values(xwind)) as n
from lidar5m
where scan_id=$1;
$$ language sql;

CREATE 
OR 
REPLACE FUNCTION n_hmrf(id int)
returns numeric AS $$
select sum(n_values(xwind)) as n
from hmrf5m
where scan_id=$1;
$$ language sql;


select scan_id, n_leosphere(scan_id),
       n_hmrf(scan_id)
from (select distinct scan_id from hmrf5m) h1;


select scan_id,
       n_leosphere,
       n_hmrf,
       n_hmrf / n_leosphere as ratio
from (
     select scan_id,
     	    n_leosphere(scan_id),
     	    n_hmrf(scan_id)
     from (select distinct scan_id from hmrf5m) h1
     ) h2;
	


\copy (select scan_id,
       n_leosphere,
       n_hmrf,
       n_hmrf / n_leosphere as ratio
from (
     select scan_id,
     	    n_leosphere(scan_id),
     	    n_hmrf(scan_id)
     from (select distinct scan_id from hmrf5m) h1
     ) h2) to 'wind_counts_5m.csv' delimiter ',' csv header
