-- functions that do cool things!

-- requires:
-- floatvec
-- aggs_for_vecs
-- aggs_for_arrays
-- (might have to use sudo to install them with pgxn)
CREATE EXTENSION floatvec;
CREATE EXTENSION aggs_for_vecs;
CREATE EXTENSION aggs_for_arrays;


-- flatten an array
CREATE 
OR 
REPLACE FUNCTION flatten(a real[])
returns real[] AS $$
select array(select unnest(a));
$$ language sql;

-- remove data where status==0
CREATE 
OR 
REPLACE FUNCTION remove_status0(a real[], s boolean[])
returns real[] AS $$
-- replace false with null then multiply, thus transforming
-- status==false values into nulls
select vec_mul(a, array_replace(s, false, null)::int[]::real[]);
$$ language sql;

-- estimate xwind values
CREATE 
OR 
REPLACE FUNCTION xwind(rws1 real[], rws3 real[], el real)
returns real[] AS $$
-- get xwind estimates from mean rws in west and east directions
select vec_div(vec_sub(rws1, rws3), (2*cos(el))::real)::real[];
$$ language sql;

-- estimate xwind variance
CREATE 
OR 
REPLACE FUNCTION xwind_var(vrws1 real[], vrws3 real[],
                           nrws1 int[], nrws3 int[])
returns real[] AS $$
-- get variance of xwind estimates
declare
n1 real[] = vec_sub(nrws1, 1)::real[];
n3 real[] = vec_sub(nrws3, 1)::real[];
begin
-- ((n1 - 1) * rws1 + (n3 - 1) * rws3) / (n1 + n3 - 2)
return vec_div(vec_add(vec_mul(n1, vrws1), vec_mul(n3, vrws3)),
               vec_add(n1, n3))::real[];
end;
$$ language plpgsql;


-- get mode from scan xml
CREATE 
OR 
REPLACE FUNCTION get_mode(xml xml)
returns text AS $$
select (xpath('//scan/@mode', xml)::text[])[1];
$$ language sql;

-- get elevation angle from scan xml
CREATE 
OR 
REPLACE FUNCTION get_el(xml xml)
returns real AS $$
select ((xpath('//scan/@elevation_angle_deg', xml)::text[])[1]::numeric * pi() / 180)::real;
$$ language sql;


-- get los rws summary stats
CREATE 
OR 
REPLACE FUNCTION rws_by_los(tmin timestamp, tmax timestamp,
                            tstep interval, id int)
returns table(scan_id smallint, "time" timestamp, los_id smallint,
              rws real[], vrws real[], nrws int[]) AS $$
select scan_id,
       time_bucket(tstep, time) as t2,
       los_id,
       vec_to_mean(remove_status0(rws, status))::real[] as rws,
       vec_to_var_samp(remove_status0(rws, status))::real[] as vrws,
       vec_to_count(remove_status0(rws, status))::int[] as nrws
from profiles
where time between tmin and tmax
      and scan_id=id
group by scan_id, t2, los_id;
$$ language sql;


-- aggregate los summaries with array_agg
CREATE 
OR 
REPLACE FUNCTION los_arr(tmin timestamp, tmax timestamp,
                         tstep interval, id int)
returns table(scan_id smallint, "time" timestamp, elevation real,
              rws real[][], vrws real[][], nrws int[][]) AS $$
select scan_id, time, elevation, rws, vrws, nrws from (
select scan_id, time,
       array_agg(rws order by los_id) as rws,
       array_agg(vrws order by los_id) as vrws,
       array_agg(nrws order by los_id) as nrws
from rws_by_los(tmin, tmax, tstep, id)
group by scan_id, time) arr1
join (select id, get_el(xml) as elevation from scans) s1
on scan_id=id
$$ language sql;


-- unnest an array by one level only
CREATE 
OR 
REPLACE FUNCTION get_los(a anyarray, i int)
returns anyarray AS $$
SELECT ARRAY(SELECT unnest(a[(i + 1):(i + 1)][:]));
$$ language sql;


-- get wind summaries
CREATE 
OR 
REPLACE FUNCTION winds(tmin timestamp, tmax timestamp,
                       tstep interval, id int)
returns table(scan_id smallint, "time" timestamp,
              x real[], xvar real[]) AS $$
SELECT scan_id, time,
       xwind(get_los(rws, 1), get_los(rws, 3), elevation),
       xwind_var(get_los(vrws, 1), get_los(vrws, 3),
                 get_los(nrws, 1), get_los(nrws, 3))
from los_arr(tmin, tmax, tstep, id);
$$ language sql;





-- get rid of anomalous numbers
CREATE 
OR 
REPLACE FUNCTION remove_bad_number(lidar_id1 int, n real)
returns void AS $$ 
BEGIN
-- set the status to false and replace the bad number with NaN
-- holy bujeezus this works???
update profiles set status=(select array_agg(case when cnr=n then false else status end order by r asc) from unnest(cnr, status) with ordinality as t(cnr, status, r)),
       cnr=array_replace(cnr, n, 'NaN')
       where profiles.lidar_id=lidar_id1 and n=any(cnr);
END;$$
language plpgsql;



-- get variance after removing outliers
CREATE 
OR 
REPLACE FUNCTION remove_outliers(arr real[])
returns real[] AS $$
DECLARE q25 real;
DECLARE q75 real;
DECLARE iqr real;
DECLARE upper_threshold real;
DECLARE lower_threshold real;
BEGIN
-- get the quartile values
select percentile_cont(.25) WITHIN GROUP (ORDER BY n asc) into q25 from unnest(arr) as n;
select percentile_cont(.75) WITHIN GROUP (ORDER BY n asc) into q75 from unnest(arr) as n;
select q75 - q25 into iqr;
select q25 - iqr * 1.5 into lower_threshold;
select q75 + iqr * 1.5 into upper_threshold;
return array_agg(n) filter (where n between lower_threshold and upper_threshold) from unnest(arr) as n;
END;$$
language plpgsql;

CREATE 
OR 
REPLACE FUNCTION robust_variance_append(arr real[], n real)
returns real[] AS $$ 
BEGIN
return array_append(arr, n);
END;$$
language plpgsql;

CREATE 
OR 
REPLACE FUNCTION robust_variance_from_array(arr real[])
returns real AS $$
BEGIN
return variance(n) from unnest(remove_outliers(arr)) as n;
END;$$
language plpgsql;

CREATE AGGREGATE robust_variance(real)
(
    sfunc = robust_variance_append,
    stype = real[],
    finalfunc = robust_variance_from_array,
    initcond = '{}'
);

-- this is a convenient array version, with syntax similar to the aggs_for_vecs package
CREATE 
OR 
REPLACE FUNCTION robust_variance_append(arr real[], arr2 real[])
returns real[] AS $$
DECLARE arr_ndim int := array_ndims(arr);
BEGIN
if arr_ndim=2 then
   return arr || arr2;
else
   return array[arr] || arr2;
end if;
END;$$
language plpgsql;

CREATE 
OR 
REPLACE FUNCTION robust_variance_from_array2d(arr real[])
returns real[] AS $$
DECLARE arr_length int := array_length(arr, 2);
DECLARE variances real[];
BEGIN
if arr_length is not null then
   -- get the variance for each slice of the arrays
   for i in 1..arr_length loop
        select array_append(variances, robust_variance_from_array(flatten(arr[1:arr_length][i:i]))) into variances;
   end loop;
end if;
return variances;
END;$$
language plpgsql;

CREATE AGGREGATE vec_to_robust_var(real[])
(
    sfunc = robust_variance_append,
    stype = real[],
    finalfunc = robust_variance_from_array2d,
    initcond = '{}'
);



-- get 5 minute summaries of non-wind profile data -- this version
-- filters the data first using status values then averages
CREATE 
OR 
REPLACE FUNCTION update_nonwind_5m(min_time timestamp, max_time timestamp, lidar_id1 int, scan_id1 int)
returns void AS $$ 
BEGIN
INSERT INTO lidar5m 
              ( 
                          scan_id, 
                          time, 
                          cnr,
			  cnr_whole,
                          drws
              ) 
  SELECT   scan_id1   AS scan_id, 
           t5m AS time, 
           array_agg(COALESCE(cnr, numeric 'NaN') order by r asc)   AS cnr,
	   array_agg(COALESCE(cnr_whole, numeric 'NaN') order by r asc)   AS cnr_whole, 
           array_agg(COALESCE(drws,  numeric 'NaN') order by r asc) AS drws
  FROM     ( 
                    SELECT   r,
		    	     -- the awkard syntax here with the
		    	     -- repeated filter is required by
		    	     -- postgres
                             CASE 
                                      WHEN count(cnr) FILTER (WHERE status)>2 THEN avg(cnr) FILTER (WHERE status)
                                      ELSE numeric 'NaN'
                             END                             AS cnr,
			     avg(cnr) FILTER (WHERE cnr!='NaN') AS cnr_whole,
                             CASE 
                                      WHEN count(drws) FILTER (WHERE status)>2 THEN avg(drws) FILTER (WHERE status)
                                      ELSE numeric 'NaN'
                             END                             AS drws,
                             t5m 
                    FROM     ( 
                                    -- unnesting everything for doing the averages 
                                    SELECT unnest(cnr)                    AS cnr, 
                                           unnest(drws)                   AS drws,
                                           unnest(status)                 AS status,
                                           generate_subscripts(status, 1) AS r, 
                                           date_trunc('hour', time) + date_part('minute', time)::int / 5 * interval '5 min' as t5m
                                    FROM   profiles
				    WHERE  time BETWEEN min_time AND    max_time + interval '5 min'
				    AND    scan_id=scan_id1
				    AND    lidar_id=lidar_id1 ) s1
                    GROUP BY r, 
                             t5m ) cnr1 
  GROUP BY t5m
  ON CONFLICT(scan_id, time) do UPDATE 
  set    cnr=excluded.cnr,
         cnr_whole=excluded.cnr_whole,
         drws=excluded.drws
  WHERE  lidar5m.time=excluded.time 
  AND    lidar5m.scan_id=scan_id1;
END;$$
language plpgsql;



-- get 5 minute summaries of *wind* profile data
CREATE 
OR 
REPLACE FUNCTION update_wind_5m(min_time timestamp, max_time timestamp, lidar_id1 int, scan_id1 int)
returns void AS $$ 
BEGIN
INSERT INTO lidar5m 
              ( 
                          scan_id, 
                          time, 
                          xwind, 
                          ywind, 
                          zwind,
			  zwind_var
              ) 
  SELECT   scan_id1   AS scan_id, 
           t5m AS time,
	   -- reorganize into arrays
           array_agg(COALESCE(xwind, numeric 'NaN') order by r asc) AS xwind, 
           array_agg(COALESCE(ywind, numeric 'NaN') order by r asc) AS ywind, 
           array_agg(COALESCE(zwind, numeric 'NaN') order by r asc) AS zwind,
	   array_agg(COALESCE(zwind_var, numeric 'NaN') order by r asc) AS zwind_var
  FROM     ( 
                    SELECT   r, 
                             -- avg doesn't work with NaN's so filter those 
                             avg(xwind) filter (WHERE xwind!='NaN') AS xwind, 
                             avg(ywind) filter (WHERE ywind!='NaN') AS ywind, 
                             avg(zwind) filter (WHERE zwind!='NaN') AS zwind,
			     variance(zwind) filter (WHERE zwind!='NaN') as zwind_var,
                             t5m 
                    FROM     ( 
                                    -- unnesting everything for doing the averages 
                                    SELECT unnest(xwind)                  AS xwind, 
                                           unnest(ywind)                  AS ywind, 
                                           unnest(zwind)                  AS zwind, 
                                           generate_subscripts(xwind, 1) AS r, 
                                           date_trunc('hour', time) + date_part('minute', time)::int / 5 * interval '5 min' as t5m
                                    FROM   wind
				    WHERE  time BETWEEN min_time AND    max_time + interval '5 min'
				    AND    scan_id=scan_id1
				    AND    lidar_id=lidar_id1 ) s1
                    GROUP BY r, 
                             t5m 
                    ORDER BY t5m, 
                             r ASC ) cnr1 
  GROUP BY t5m 
  ON CONFLICT (scan_id, time) do UPDATE 
  set    xwind=excluded.xwind, 
         ywind=excluded.ywind, 
         zwind=excluded.zwind,
	 zwind_var=excluded.zwind_var
  WHERE  lidar5m.time=excluded.time 
  AND    lidar5m.scan_id=scan_id1;
END;$$
language plpgsql;




-- get 5 minute summaries of non-wind profile data -- applies to all
-- lidars
CREATE 
OR 
REPLACE FUNCTION update_all_nonwind_5m()
returns void AS $$ 
BEGIN
-- (adjust this function as desired)
-- delete old estimates
update lidar5m set cnr=null, drws=null where scan_id in (select id from scans where lidar_id=18) and time>'2017-09-01';
delete from lidar5m where cnr is null and xwind is null and scan_id in (select id from scans where lidar_id=18) and time>'2017-09-01';
-- insert all the new 5-minute wind estimates into it
INSERT INTO lidar5m
              ( 
                          scan_id, 
                          time, 
                          cnr, 
                          drws
              ) 
  SELECT   scan_id, 
           t5m AS time, 
           array_agg(COALESCE(cnr, numeric 'NaN') order by r asc)   AS cnr,
	   array_agg(COALESCE(cnr_whole, numeric 'NaN') order by r asc)   AS cnr_whole, 
           array_agg(COALESCE(drws,  numeric 'NaN') order by r asc) AS drws
  FROM     ( 
                    SELECT   r,
		    	     -- the awkard syntax here with the
		    	     -- repeated filter is required by
		    	     -- postgres as of 9.6
                             CASE 
                                      WHEN count(cnr) FILTER (WHERE status and cnr!='NaN')>2 THEN avg(cnr) FILTER (WHERE status and cnr!='NaN')
                                      ELSE numeric 'NaN'
                             END                             AS cnr,
			     CASE 
                                      WHEN avg(cnr)>-30 THEN avg(cnr)
                                      ELSE numeric 'NaN'
                             END                             AS cnr_whole,
                             CASE 
                                      WHEN count(drws) FILTER (WHERE status and drws!='NaN')>2 THEN avg(drws) FILTER (WHERE status and drws!='NaN')
                                      ELSE numeric 'NaN'
                             END                             AS drws,
                             t5m,
			     scan_id
                    FROM     ( 
                                    -- unnesting everything for doing the averages 
                                    SELECT unnest(cnr)                    AS cnr, 
                                           unnest(drws)                   AS drws,
                                           unnest(status)                 AS status,
                                           generate_subscripts(status, 1) AS r, 
                                           date_trunc('hour', time) + date_part('minute', time)::int / 5 * interval '5 min' as t5m,
					   scan_id
                                    FROM   profiles where lidar_id=18 and time>'2017-09-01') s1
                    GROUP BY r, 
                             t5m,
			     scan_id) cnr1 
  GROUP BY t5m,
  	   scan_id
  ON conflict (scan_id, time) do UPDATE 
  set    cnr=excluded.cnr, 
         drws=excluded.drws
  WHERE  lidar5m.time=excluded.time 
  AND    lidar5m.scan_id=excluded.scan_id;
END;$$
language plpgsql;

INSERT INTO lidar5m
              ( 
                          scan_id, 
                          time, 
                          zwind_var
              ) 
  SELECT   scan_id, 
           t5m AS time, 
           array_agg(COALESCE(zwind_var, numeric 'NaN') order by r asc)   AS zwind_var
  FROM     ( 
                    SELECT   r,
		    	     -- the awkard syntax here with the
		    	     -- repeated filter is required by
		    	     -- postgres as of 9.6
			     variance(zwind) as zwind_var,
                             t5m,
			     scan_id
                    FROM     ( 
                                    -- unnesting everything for doing the averages 
                                    SELECT unnest(zwind)                  AS zwind,
                                           generate_subscripts(zwind, 1)  AS r, 
                                           date_trunc('hour', time) + date_part('minute', time)::int / 5 * interval '5 min' as t5m,
					   scan_id
                                    FROM   wind) s1
                    GROUP BY r, 
                             t5m,
			     scan_id) cnr1 
  GROUP BY t5m,
  	   scan_id
  ON conflict (scan_id, time) do UPDATE 
  set    zwind_var=excluded.zwind_var
  WHERE  lidar5m.time=excluded.time 
  AND    lidar5m.scan_id=excluded.scan_id;



-- get 5 minute summaries of *wind* profile data -- applies to all
-- lidars
CREATE 
OR 
REPLACE FUNCTION update_all_wind_5m()
returns void AS $$ 
BEGIN
-- (adjust this function as desired)
-- delete old estimates
update lidar5m set xwind=null, ywind=null, zwind=null where scan_id in (select id from scans where lidar_id=18) and time>'2017-09-01';
delete from lidar5m where cnr is null and xwind is null and scan_id in (select id from scans where lidar_id=18) and time>'2017-09-01';
-- insert all the new 5-minute wind estimates into it
INSERT INTO lidar5m 
              ( 
                          scan_id, 
                          time, 
                          xwind, 
                          ywind, 
                          zwind 
              ) 
  SELECT   scan_id, 
           t5m AS time,
	   -- reorganize into arrays
           array_agg(COALESCE(xwind, 'NaN') order by r asc) AS xwind, 
           array_agg(COALESCE(ywind, 'NaN') order by r asc) AS ywind, 
           array_agg(COALESCE(zwind, 'NaN') order by r asc) AS zwind 
  FROM     ( 
                    SELECT   r, 
                             -- avg doesn't work with NaN's so filter those 
                             avg(xwind) filter (WHERE xwind!='NaN') AS xwind, 
                             avg(ywind) filter (WHERE ywind!='NaN') AS ywind, 
                             avg(zwind) filter (WHERE zwind!='NaN') AS zwind, 
                             t5m,
			     scan_id
                    FROM     ( 
                                    -- unnesting everything for doing the averages 
                                    SELECT unnest(xwind)                  AS xwind, 
                                           unnest(ywind)                  AS ywind, 
                                           unnest(zwind)                  AS zwind, 
                                           generate_subscripts(xwind, 1) AS r, 
                                           date_trunc('hour', time) + date_part('minute', time)::int / 5 * interval '5 min' as t5m,
					   scan_id
                                    FROM   wind
				    WHERE lidar_id=18 and time>'2017-09-01' ) s1
                    GROUP BY r, 
                             t5m,
			     scan_id) cnr1 
  GROUP BY t5m,
  	   scan_id
  ON conflict (scan_id, time) do UPDATE 
  set    xwind=excluded.xwind, 
         ywind=excluded.ywind, 
         zwind=excluded.zwind 
  WHERE  lidar5m.time=excluded.time 
  AND    lidar5m.scan_id=excluded.scan_id;
END;$$
language plpgsql;



-- get 5 minute summaries of non-wind profile data-- this version
-- averages first and then applies a cutoff (not being used)
-- CREATE 
-- OR 
-- REPLACE FUNCTION update_nonwind_5m(min_time timestamp, max_time timestamp, lidar_id1 int, scan_id1 int)
-- returns void AS $$ 
-- BEGIN
-- INSERT INTO lidar5m 
--               ( 
--                           lidar_id, 
--                           scan_id, 
--                           time, 
--                           cnr, 
--                           drws
--               ) 
--   SELECT   lidar_id1  AS lidar_id, 
--            scan_id1   AS scan_id, 
--            lower(t5m) AS time, 
--            array_agg( 
--            CASE 
--                     WHEN cnr<-30 THEN numeric 'NaN' 
--                     ELSE COALESCE(cnr, numeric 'NaN') 
--            END)                                     AS cnr, 
--            array_agg(COALESCE(drws,  numeric 'NaN')) AS drws
--   FROM     ( 
--                     SELECT   r, 
--                              avg(cnr)  AS cnr, 
--                              avg(drws) AS drws,
--                              t5m 
--                     FROM     ( 
--                                     -- unnesting everything for doing the averages 
--                                     SELECT unnest(cnr)                    AS cnr, 
--                                            unnest(drws)                   AS drws,
--                                            generate_subscripts(status, 1) AS r, 
--                                            t5m 
--                                     FROM   ( 
--                                                   SELECT * 
--                                                   FROM   ( 
--                                                                 -- the time ranges 
--                                                                 SELECT tsrange(t1, t1 + interval '5 min')                         AS t5m
--                                                                 FROM   generate_series(min_time, max_time, '5 minutes'::interval) AS t1 ) t
--                                                   JOIN 
--                                                          ( 
--                                                                 -- the time slice 
--                                                                 SELECT * 
--                                                                 FROM   profiles 
--                                                                 WHERE  time BETWEEN min_time AND    max_time + interval '5 min'
--                                                                 AND    scan_id=scan_id1 
--                                                                 AND    lidar_id=lidar_id1 ) p 
--                                                   ON     t5m@>time ) p1 ) s1 
--                     GROUP BY r, 
--                              t5m 
--                     ORDER BY t5m, 
--                              r ASC ) cnr1 
--   GROUP BY t5m 
--   ON conflict (scan_id, time) do UPDATE 
--   set    cnr=excluded.cnr, 
--          drws=excluded.drws
--   WHERE  lidar5m.time=excluded.time 
--   AND    lidar5m.lidar_id=lidar_id1 
--   AND    lidar5m.scan_id=scan_id1;
-- END;$$
-- language plpgsql;








-- get 5 minute summary of both the nonwind and wind profile data
CREATE 
OR 
REPLACE FUNCTION update_lidar5m(min_time timestamp, max_time timestamp, lidar_id1 int, scan_id1 int)
returns void AS $$ 
BEGIN
PERFORM update_nonwind_5m(min_time, max_time, lidar_id1, scan_id1);
-- I should find out if the scanning mode is dbs or not first. This only matters for dbs mode!
PERFORM update_wind_5m(min_time, max_time, lidar_id1, scan_id1);
END;$$
language plpgsql;




-- update lidar 30 minute summary data 
CREATE 
OR 
REPLACE FUNCTION update_lidar30m(min_time timestamp, max_time timestamp, lidar_id1 int, scan_id1 int)
returns void AS $$ 
BEGIN
INSERT INTO lidar30m 
              ( 
                          scan_id, 
                          time, 
                          tke 
              ) 
  SELECT   scan_id1                                                        AS scan_id, 
           Lower(t5m)                                                      AS time, 
           array_agg(COALESCE((xwind + ywind + zwind) / 2, numeric 'NaN')) AS tke 
  FROM     ( 
                    SELECT   r, 
                             -- have to use nullif here to transform NaN into null 
                             variance(xwind) filter (WHERE xwind!=numeric 'NaN' 
                                                     AND xwind BETWEEN -35 AND 35) AS xwind, 
                             variance(ywind) filter (WHERE ywind!=numeric 'NaN' 
                                                     AND ywind BETWEEN -35 AND 35) AS ywind, 
                             variance(zwind) filter (WHERE zwind!=numeric 'NaN') AS zwind, 
                             t5m 
                    FROM     ( 
                                    -- unnesting everything for doing the averages 
                                    SELECT unnest(xwind)                  AS xwind, 
                                           unnest(ywind)                  AS ywind, 
                                           unnest(zwind)                  AS zwind, 
                                           generate_subscripts(xwind, 1) AS r, 
                                           t5m 
                                    FROM   ( 
                                                  SELECT * 
                                                  FROM   ( 
                                                                -- the time ranges 
                                                                SELECT tsrange(t1, t1 + interval '30 min')                    AS t5m
                                                                FROM   generate_series(min_time, max_time, interval '30 min') AS t1 ) t
                                                  JOIN 
                                                         ( 
                                                                -- the time slice 
                                                                SELECT * 
                                                                FROM   wind
                                                                WHERE  time BETWEEN min_time AND    max_time + interval '30 min'
                                                                AND    scan_id=scan_id1 
                                                                AND    lidar_id=lidar_id1 ) p 
                                                  ON     t5m@>time ) p1 ) s1 
                    GROUP BY r, 
                             t5m 
                    ORDER BY t5m, 
                             r ASC ) cnr1 
  GROUP BY t5m 
  ON conflict (scan_id, time) do UPDATE 
  set    tke=excluded.tke 
  WHERE  lidar30m.time=excluded.time 
  AND    lidar30m.scan_id=scan_id1;
END;$$
language plpgsql;



-- update lidar 30 minute summary data (better, faster version)
CREATE 
OR 
REPLACE FUNCTION update_lidar30m(min_time timestamp, max_time timestamp, scan_id1 int, fixed_pbl boolean)
returns void AS $$ 
BEGIN
-- uses vertical wind speeds, therefore only applies to data in DBS
-- mode
INSERT INTO lidar30m (scan_id, time, zwind_var, n)
SELECT scan_id,
       time_bucket(interval '30 min', time) as t30,
       -- get the variance when there are enough measurements for it
       -- to be meaningful
       vec_to_var_samp(rws) AS zwind_var,
       vec_to_count(rws) as n
FROM (
      -- the time slice 
      SELECT scan_id,
      	     time,
	     remove_status0(rws, status) as rws
      FROM   profiles
      WHERE  time BETWEEN min_time AND max_time + interval '30 min'
      AND    scan_id=scan_id1
      AND    (los_id=4 or fixed_pbl)
     ) p1
GROUP BY scan_id, t30
ON conflict (scan_id, time) do UPDATE
set zwind_var=excluded.zwind_var, n=excluded.n;
END;$$
language plpgsql;

-- update all 30 minute lidar summary data
CREATE 
OR 
REPLACE FUNCTION update_all_lidar_30m()
returns void AS $$ 
BEGIN
-- update_lidar30m only applies to data in DBS mode
perform update_lidar30m(min_time, max_time, scan_id, fixed_pbl)
from (
      select scan_id,
      	     min(time) as min_time,
	     max(time) as max_time,
	     (
	      select xpath_exists('//scan[@mode="FixedForPBLTrajectory"]', xml)
	      from scans
	      where scans.id=scan_id
	     ) as fixed_pbl
      from profiles
      where scan_id in (
      	    	        select id
			from scans
			where xpath_exists('//scan[@mode="dbs"]', xml)
			      or xpath_exists('//scan[@mode="FixedForPBLTrajectory"]', xml)
		       )
      group by scan_id
     ) p1;
END;$$
language plpgsql;




-- update lidar 15 minute summary data 
CREATE 
OR 
REPLACE FUNCTION update_lidar15m(min_time timestamp, max_time timestamp, scan_id1 int)
returns void AS $$
DECLARE scan_mode text;
DECLARE is_pbl boolean;
DECLARE has_zwind boolean;
BEGIN
-- get the scanning mode
select get_mode(xml) into scan_mode from scans where id=scan_id1;
-- check if the data has vertical wind speeds
select scan_mode in ('FixedForPBLTrajectory', 'dbs') into has_zwind;
-- check to see if the data is from a fixedforpbl scanning mode
select scan_mode='FixedForPBLTrajectory' into is_pbl;
-- update the 15m table
if has_zwind then
    INSERT INTO lidar15m (scan_id, time, cnr_whole, zwind_var, zwind_n)
    SELECT scan_id,
	   time_bucket(interval '15 min', time) as t15,
	   vec_to_mean(cnr) AS cnr_whole,
	   vec_to_robust_var(rws1) filter (WHERE los_id=4 or is_pbl) AS zwind_var,
	   vec_to_count(rws1) filter (WHERE los_id=4 or is_pbl) as n
    FROM (
	  -- the time slice 
	  SELECT *,
		 remove_status0(rws, status) as rws1
	  FROM   profiles
	  WHERE  time BETWEEN min_time AND max_time + interval '15 min'
	  AND    scan_id=scan_id1
	 ) p1
    GROUP BY scan_id, 
	     t15
    ON conflict (scan_id, time) do UPDATE 
    set cnr_whole=excluded.cnr_whole, zwind_var=excluded.zwind_var, zwind_n=excluded.zwind_n;
else
    INSERT INTO lidar15m (scan_id, time, cnr_whole, zwind_var, zwind_n)
    SELECT scan_id,
	   time_bucket(interval '15 min', time) as t15,
	   vec_to_mean(cnr) AS cnr_whole,
	   null AS zwind_var,
	   null as n
    FROM (
	  -- the time slice 
	  SELECT time,
		 scan_id,
		 cnr
	  FROM   profiles
	  WHERE  time BETWEEN min_time AND max_time + interval '15 min'
	  AND    scan_id=scan_id1
	 ) p1
    GROUP BY scan_id, 
	     t15
    ON conflict (scan_id, time) do UPDATE 
    set cnr_whole=excluded.cnr_whole, zwind_var=excluded.zwind_var, zwind_n=excluded.zwind_n;
end if;
END;$$
language plpgsql;

-- update all 15 minute lidar summary data
CREATE 
OR 
REPLACE FUNCTION update_all_lidar_15m()
returns void AS $$ 
BEGIN
perform update_lidar15m(min_time, max_time, scan_id)
from (
      select scan_id,
      	     min(time) as min_time,
	     max(time) as max_time
      from profiles
      group by scan_id
     ) p1;
END;$$
language plpgsql;


select scan_id, time_bucket(interval '15 min', time) as t15, vec_to_mean(cnr) AS cnr_whole from (select * from profiles limit 10) p1 group by scan_id, t15;

-- for grouping
select scan_id, min(time), max(time) from profiles group by scan_id;
