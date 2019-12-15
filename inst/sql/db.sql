-- miscellaneous stuff that I need to organize


-- get lidar scan resolutions (laser resolutions, not to be confused
-- with the resolution of the recorded data)
select res, count(*) from (select substring(name, '[_ ](\d*)m[_ ]')::integer as res from (select (xpath('//lidar_config/@name', xml)::text[])[1] as name from lidar_configs) c1) c2 group by res;
substring((xpath('//lidar_config/@name', xml)::text[])[1], '[_ ](\d*)m[_ ]')::integer


-- timescaledb bug?
-- lidar 5 minute summary data (the default timescaledb interval of 1 month should be fine here)
create table summary5m2 (scan_id smallint not null, time timestamp not null, cnr real[], drws real[], xwind real[], ywind real[], zwind real[], primary key(scan_id, time), foreign key(scan_id) references scans);
select create_hypertable('summary5m2', 'time');
alter table summary5m2 rename to lidar5m2;

alter table lidar5m2 drop constraint summary5m2_pkey;
alter table lidar5m2 add constraint lidar5m2_pkey primary key(scan_id, time);

insert into lidar5m2 (scan_id, time, cnr) values (1, '2017-11-19 17:00:00'::timestamp, '{}') on conflict (scan_id, time) do update set scan_id=999;

insert into lidar5m2 (scan_id, time, cnr) values (1, '2017-11-19 17:00:00'::timestamp, '{}'),(2, '2017-11-19 19:00:00'::timestamp, '{}') on conflict (scan_id, time) do update set scan_id=99;

select * from lidar5m2;

drop table lidar5m2;


alter table scans add column id2 serial;
update scans set id=id2;

alter table lidar30m drop constraint lidar30m_lidar_id_fkey;
alter table lidar30m add constraint lidar30m_scans_fk foreign key (lidar_id, scan_id) references scans(lidar_id, id) on update cascade;

alter table wind drop constraint wind_lidar_id_fkey;
alter table wind add constraint winds_scans_fk foreign key (lidar_id, scan_id) references scans(lidar_id, id) on update cascade;

alter table profiles add constraint profiles_scans_fk foreign key (lidar_id, scan_id) references scans(lidar_id, id) on update cascade;



-- look for anomalous numbers
with totals as (select lidar_id, cnr::numeric as cnr, count(*) from (select lidar_id, unnest(cnr) as cnr from profiles where time>'2017-10-11') p1 group by lidar_id, cnr) select m1.lidar_id, cnr, m1.count from (select lidar_id, max(count) as count from totals group by lidar_id) m1 join totals on m1.lidar_id=totals.lidar_id and m1.count=totals.count;

-- look more closely at CESTM_roof-14 and -2.56041
select date_trunc('month', time) as month, count(*) from (select time, unnest(cnr) as cnr from profiles where lidar_id=1) p1 where cnr=(-2.56041)::real group by month;

select date(time) as date, count(*) from (select time, unnest(cnr) as cnr from profiles where lidar_id=1 and time>'2017-08-01' and time<'2017-10-01') p1 where cnr=(-2.56041)::real group by date;


select array_agg(case when cnr='NaN' then false else status end order by r asc) from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) AS r from profiles where lidar_id=1 and (-2.56041)::real=any(cnr));

-- select time, cnr from profiles where lidar_id=1 and (-2.56041)::real=any(cnr) limit 1;
select time, cnr from profiles where lidar_id=1 and time>'2017-10-28' and 'NaN'=any(cnr) limit 1;
select time, cnr, status from profiles where lidar_id=1 and time='2017-10-28 00:18:55.611674';
select array_agg(case when cnr='NaN' then false else status end order by r asc) as status from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) AS r from profiles where lidar_id=1 and time='2017-10-28 00:18:55.611674') p1;

update profiles set status=



update profiles set status=p2.status from (select time, array_agg(case when cnr='NaN' then false else status end order by r asc) as status from (select time, unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) AS r from profiles where lidar_id=1 and 'NaN'=any(cnr) and time>'2017-08-01') p1 group by time) p2 where profiles.lidar_id=1 and profiles.time=p2.time and 'NaN'=any(cnr);


-- move data from profiles to wind
insert into wind select lidar_id, scan_id, time, xwind, ywind, zwind from profiles where time between '2016-03-16' and '2016-07-01' and xwind is not null;
insert into wind select lidar_id, scan_id, time, xwind, ywind, zwind from profiles where xwind is not null;

alter table profiles drop xwind, drop ywind, drop zwind;


-- take a look at timescaledb table sizes
SELECT * FROM hypertable_relation_size_pretty('profiles');

insert into summary5m (lidar_id, scan_id, time, cnr, drws) select * from cnr_new ON conflict (lidar_id, scan_id, time) do UPDATE set cnr=excluded.cnr, drws=excluded.drws where summary5m.lidar_id=excluded.lidar_id and summary5m.scan_id=excluded.scan_id and summary5m.time=excluded.time;

select count(*) from lidar5m where lidar_id=26 and scan_id=17 and time between '2017-10-28' and '2017-10-29';

update summary5m s set cnr=c.cnr, drws=c.drws from cnr_new c where s.lidar_id=c.lidar_id and s.scan_id=c.scan_id and s.time=c.time

-- take a look at intermediate confidence values per lidar/scan
select stid, scan_id, count from (select lidar_id, scan_id, count(*) from (select lidar_id, scan_id, unnest(confidence) as confidence from profiles where time>'2017-10-28') p1 where confidence<100 and confidence>0 group by lidar_id, scan_id) p2 join lidars on lidars.id=p2.lidar_id;

-- just look at cestm14
select scan_id, date(time) as date, count(*) from (select scan_id, time, unnest(confidence) as confidence from profiles where time>'2017-10-10' and lidar_id=1) p1 where confidence<100 and confidence>0 group by scan_id, date order by date;

-- useful notes for handling arrays, xml, timescaledb:
select min(time) from profiles join (select *, xpath_exists('//scan[@mode="dbs"]', xml) as dbs from scans) s0 on profiles.lidar_id=s0.lidar_id and scan_id=id where dbs;

select min_cnr, p1.id, name from (select min(array_to_min(remove_status0(cnr, status))) as min_cnr, scan_id as id from profiles group by id) p1 join (select id, (xpath('//scan/@name', xml)::varchar[])[1] as name from scans) s1 on p1.id=s1.id order by min_cnr asc;

-- xml!
xpath_exists('//scan/[@distances_m]', xml)
-- search for matching scan mode
select xpath_exists('//scan[@mode="dbs"]', xml) from scans limit 5;
-- look at modes
select distinct (xpath('//scan/@mode', xml)::varchar[])[1] as mode from scans;
select count(*), (xpath('//scan/@mode', xml)::varchar[])[1] as mode from scans group by mode;
-- find lowest CNR for each scanning mode
select id, (xpath('//scan/@name', xml)::varchar[])[1] as name from scans group by id order by min_cnr asc;
select min_cnr, p1.id, name from (select min(array_to_min(remove_status0(cnr, status))) as min_cnr, scan_id as id from profiles group by id) p1 join (select id, (xpath('//scan/@name', xml)::varchar[])[1] as name from scans) s1 on p1.id=s1.id order by min_cnr asc;

-- won't work, must add trigger instead
alter table wind add constraint is_dbs check(select xpath_exists('//scan[@mode="dbs"]', xml) from scans where id=scan_id);
alter table wind add constraint is_dbs check(scan_id in (select id from scans where xpath_exists('//scan[@mode="dbs"]', xml)));

-- arrays!
-- ordinality
select * from unnest(array[14,41,7]) with ordinality;
-- trying out element-wise sums
select array (select sum(elem) from tbl t, unnest(t.arr) with ordinality x(elem, rn) group by rn order by rn);

-- a 5 minute average!
select array (select avg(elem) from (select * from profiles where time between '2017-01-01' and '2017-01-01 00:05' and lidar_id=5) t, unnest(t.cnr) with ordinality x(elem, rn) group by rn,  order by rn);
-- time series
select * from generate_series('2017-01-01'::timestamp,'2017-01-04'::timestamp,'1 day'::interval);
-- element-wise mean
select array (select avg(elem) from (select * from profiles limit 100) as t, unnest(t.cnr) with ordinality x(elem, rn) group by rn order by rn);
-- ?
-- select avg(cnr[1]) from profiles group by ;
select array (select avg(elem) from profiles as t, unnest(t.rws) with ordinality x(elem, rn) group by rn, los_id order by rn);

-- avg by range
select r, avg(cnr), count(*) from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r from profiles where time between '2017-04-26 14:00' and '2017-04-26 14:05' and lidar_id=27) s1 where status group by r order by r asc;
x-- back to array
select array_agg(cnr) from (select r, avg(cnr) as cnr, count(*) from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r from profiles where time between '2017-04-26 14:00' and '2017-04-26 14:05' and lidar_id=27) s1 where status group by r order by r asc) cnr1;
-- join with indices
select array_agg(cnr) from (select generate_subscripts(cnr, 1) from ) join (select r, avg(cnr) as cnr, count(*) from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r from profiles where time between '2017-04-26 14:00' and '2017-04-26 14:05' and lidar_id=27) s1 where status group by r order by r asc) cnr1;
-- join with indices
select array_agg(cnr) from (select r, avg(case when status then cnr else null end) as cnr, count(*) from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r from profiles where time between '2017-04-26 14:00' and '2017-04-26 14:05' and lidar_id=27) s1 group by r order by r asc) cnr1;
-- split into intervals
select * from (select generate_series as t5m from generate_series('2017-04-26 14:00'::timestamp,'2017-04-26 14:05'::timestamp,'5 minutes'::interval)) t join (select * from profiles where time between '2017-04-26 14:00' and '2017-04-26 14:05' and lidar_id=27) p on time-t5m<interval '5 minutes';
-- interval averages
select array_agg(cnr) from (select r, avg(case when status then cnr else null end) as cnr, count(*) from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r, t5m as time from (select * from (select generate_series as t5m from generate_series('2017-04-26 14:00'::timestamp,'2017-04-26 14:05'::timestamp,'5 minutes'::interval)) t join (select * from profiles where time between '2017-04-26 14:00' and '2017-04-26 14:10' and lidar_id=27) p on time-t5m<interval '5 minutes') p1) s1 group by r, time order by time, r asc) cnr1;
-- whoooooooaaaaaaa omfg hoooooly shiiit
select time, array_agg(cnr) as cnr from (select r, avg(case when status then cnr else null end) as cnr, count(*), time from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r, t5m as time from (select * from (select generate_series as t5m from generate_series('2017-04-25'::timestamp,'2017-04-26'::timestamp,'5 minutes'::interval)) t join (select * from profiles where time between '2017-04-25' and '2017-04-26' and lidar_id=27) p on time>=t5m and time-t5m<interval '5 minutes') p1) s1 group by r, time order by time, r asc) cnr1 group by time;
-- time range version-- whoohoo!
select lower(t5m) as time, array_agg(cnr) as cnr from (select r, avg(case when status then cnr else null end) as cnr, count(*), t5m from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r, t5m from (select * from (select tsrange(t1, t1 + interval '5 min') as t5m from generate_series('2017-04-25'::timestamp,'2017-04-26'::timestamp,'5 minutes'::interval) as t1) t join (select * from profiles where time between '2017-04-25' and '2017-04-26' and lidar_id=27) p on t5m@>time) p1) s1 group by r, t5m order by t5m, r asc) cnr1 group by t5m;




















create trigger update_lidar5m after insert or update on profiles execute procedure update_lidar5m();


select lower(t5m) as time, array_agg(cnr) as cnr from (select r, avg(case when status then cnr else null end) as cnr, count(*), t5m from (select unnest(cnr) as cnr, unnest(status) as status, generate_subscripts(status, 1) as r, t5m from (select * from (select tsrange(t1, t1 + interval '5 min') as t5m from generate_series('2017-04-25'::timestamp,'2017-04-26'::timestamp,'5 minutes'::interval) as t1) t join (select * from profiles where time between '2017-04-25' and '2017-04-26' and lidar_id=27) p on t5m@>time) p1) s1 group by r, t5m order by t5m, r asc) cnr1 group by t5m;














-- forget all this, it's ridiculous

-- getting z wind speed
select *, case when los_id=4 then (1/(4*sin(elevation)^2+1))*rws[1] else (sin(elevation)/(4*sin(elevation)^2+1))*rws[1] end as z0 from profiles limit 5;
-- aggregating
select sum(z0) over (rows between 2 preceding and 2 following) from (select *, case when los_id=4 then (1/(4*sin(elevation)^2+1))*rws[1] else (sin(elevation)/(4*sin(elevation)^2+1))*rws[1] end as z0 from profiles limit 10) as z;
-- filter out bad data
select time, zwind from (select *, case when (count(*) filter (where status[1]) over (partition by lidar_id, scan_id rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id rows between 2 preceding and 2 following) else null end as zwind from (select *, case when los_id=4 then (1/(4*sin(elevation)^2+1))*rws[1] else (sin(elevation)/(4*sin(elevation)^2+1))*rws[1] end as z0 from profiles where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z) z1 where zwind is not null limit 30;
-- entire profile
select time, array_agg(cnr) as cnr, array_agg(drws) as drws, array_agg(zwind) as zwind
from (
select *, case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind
from (
select lidar_id, scan_id, time, (case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0, unnest(status) as status2, generate_subscripts(status, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z
where rid<3) z1
group by time limit 3;

-- entire profile w/ everything
select time, array_agg(cnr) as cnr, array_agg(drws) as drws, array_agg(zwind) as zwind
from (
select *, case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind
from (
select lidar_id, scan_id, time, unnest(cnr) as cnr,
       unnest(drws) as drws,
       (case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0,
       unnest(status) as status2, generate_subscripts(status, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z
where rid<3) z1
group by time limit 3;


-- just winds

select count(distinct los_id) filter(where status and los_id between 0 and 4) over (partition by lidar_id, scan_id, rid rows between 4 preceding and current row) from (select lidar_id, scan_id, los_id, time, unnest(status) as status,
generate_subscripts(cnr, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z limit 5;

select max(rank()) filter(where status and los_id between 0 and 4) over (partition by lidar_id, scan_id, rid rows between 4 preceding and current row) from (select lidar_id, scan_id, los_id, time, unnest(status) as status,
generate_subscripts(cnr, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z limit 5;

select count(*) filter(where status and los_id between 0 and 4) over (partition by lidar_id, scan_id, rid rows between 4 preceding and current row) from (select lidar_id, scan_id, los_id, time, unnest(status) as status,
generate_subscripts(cnr, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z limit 5;


-- wind estimate function
create or replace function est_wind() returns void as $$
begin
-- what I should get here:
-- min/max time
-- min/max sequence_id
update profiles set xwind='{1}'::int[] from (
select * from (
	-- creating indices to hold DBS data in the correct order
	select * from (
	       select generate_series(min(sequence_id) - 1, max(sequence_id)) as sequence_id from profiles where time between '2016-03-18' and '2016-03-18 00:10'
	       ) s0, (
	       	 select generate_series(0, 4) as los_id
	       ) l0
	) dbs join (
	  -- get profile data, with an extra 5 minutes at the start to
	  -- get the previous sequence
	  select * from profiles where time between '2016-03-18'::timestamp - interval '5 minutes' and '2016-03-18 00:10'
	) p1 on p1.sequence_id=dbs.sequence_id and p1.los_id=dbs.los_id
-- 	order by p1.sequence_id desc, p1.los_id desc
) dbs1
where profiles.time=dbs1.time and profiles.lidar_id=dbs1.lidar_id;
end;
$$ LANGUAGE plpgsql;

-- update profiles set xwind='{1}'::int[] from (
update profiles set xwind=dbs1.cnr from (
select lidar_id, time, cnr from (
-- creating indices to hold DBS data in the correct order
select * from (
select generate_series(min(sequence_id) - 1, max(sequence_id)) as sequence_id from profiles where time between '2016-03-18' and '2016-03-18 00:10'
) s0, (
select generate_series(0, 4) as los_id
) l0
) dbs join (
-- get profile data, with an extra 5 minutes at the start to
-- get the previous sequence
select lidar_id, sequence_id, los_id, time, cnr from profiles where time between '2016-03-18'::timestamp - interval '5 minutes' and '2016-03-18 00:10'
) p1 on p1.sequence_id=dbs.sequence_id and p1.los_id=dbs.los_id
-- 	order by p1.sequence_id desc, p1.los_id desc
) dbs1
where profiles.time=dbs1.time and profiles.lidar_id=dbs1.lidar_id;

select count(*) from (
select * from (
select generate_series(min(sequence_id) - 1, max(sequence_id)) as sequence_id from profiles where time between '2016-03-18' and '2016-03-18 00:10'
) s0, (
select generate_series(0, 4) as los_id
) l0
) dbs join (
-- get profile data, with an extra 5 minutes at the start to
-- get the previous sequence
select * from profiles where time between '2016-03-18'::timestamp - interval '5 minutes' and '2016-03-18 00:10'
) p1 on p1.sequence_id=dbs.sequence_id and p1.los_id=dbs.los_id
where lidar_id is null;

-- creating indices to hold DBS data in the correct order
select lidar_id from (
(select * from (select generate_series(min(sequence_id) - 1, max(sequence_id)) as sequence_id from (select * from profiles where time between '2016-03-18' and '2016-03-18 00:10') p0) s0, (select generate_series(0, 4) as los_id) l0
) dbs join (
-- get profile data, with an extra 5 minutes at the start to
-- get the previous sequence
select * from profiles where time between '2016-03-18'::timestamp - interval '5 minutes' and '2016-03-18 00:10'
) p1 on p1.sequence_id=dbs.sequence_id and p1.los_id=dbs.los_id
-- 	order by p1.sequence_id desc, p1.los_id desc
) dbs1;

update profiles set xwind='{1}'::int[] from (
(
-- creating indices to hold DBS data in the correct order
(
select * from (
select generate_series(min(sequence_id) - 1, max(sequence_id)) as sequence_id from profiles where time between '2016-03-18' and '2016-03-18 00:10'
) s0, (
select generate_series(0, 4) as los_id
) l0
) dbs join (
-- get profile data, with an extra 5 minutes at the start to
-- get the previous sequence
select * from profiles where time between '2016-03-18'::timestamp - interval '5 minutes' and '2016-03-18 00:10'
) p1 on p1.sequence_id=dbs.sequence_id and p1.los_id=dbs.los_id
-- 	order by p1.sequence_id desc, p1.los_id desc
) dbs1
where profiles.time=dbs1.time and profiles.lidar_id=dbs1.lidar_id;

update profiles set xwind='{1}'::int[] from (
       select * from profiles where time between '2016-03-18'::timestamp - interval '5 minutes' and '2016-03-18 00:10'
       ) dbs1
where profiles.time=dbs1.time and profiles.lidar_id=dbs1.lidar_id;

create or replace function estimate_wind() returns trigger as $$
begin
perform generate_series(min(sequence_id) - 1, max(sequence_id)) from (select generate_series(0, 4) as sequence_id) l0;
end;
$$ LANGUAGE plpgsql;


-- Check that empname and salary are given
IF NEW.empname IS NULL THEN
RAISE EXCEPTION 'empname cannot be null';
END IF;
IF NEW.salary IS NULL THEN
RAISE EXCEPTION '% cannot have null salary', NEW.empname;
END IF;

-- Who works for us when they must pay for it?
IF NEW.salary < 0 THEN
RAISE EXCEPTION '% cannot have a negative salary', NEW.empname;
END IF;

-- Remember who changed the payroll when
NEW.last_date := current_timestamp;
NEW.last_user := current_user;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- something like this, goodness gracious
-- re-aggregate data
select lidar_id, scan_id, time,
array_agg(zwind) as zwind from (
select *,
-- aggregate wind numbers into wind estimates where applicable.
-- get only profiles that are in the current or previous sequence,
-- with all 5 LOS IDs
-- case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end

case when (count(distinct los_id)=5 filter(where status2 and los_id between 0 and 4) over (partition by lidar_id, scan_id, rid rows between 4 preceding and current row)) then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind from (
-- unnest the raw data,
-- calculate wind numbers
select lidar_id, scan_id, time,
(case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0,
unnest(status) as status2,
generate_subscripts(status, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z
) z1
group by lidar_id, scan_id, time
) w1
) p
on t5m@>time
) p1
) s1;

-- winds + 5min summary together
-- aggregate again, get start times of 5 minute intervals
select lidar_id, scan_id, lower(t5m) as time, array_agg(cnr) as cnr,
       array_agg(drws) as drws, array_agg(zwind) as zwind from
       -- get average of numbers where status is true
       (select lidar_id, scan_id, t5m, r,
       	       avg(case when status then cnr else null end) as cnr,
               avg(case when status then drws else null end) as drws,
	       avg(case when status then zwind else null end) as zwind from
	       -- unnest everything again so we can get averages
	       (select lidar_id, scan_id, t5m,
	       	       unnest(cnr) as cnr, unnest(drws) as drws,
	               unnest(zwind) as zwind, unnest(status) as status,
		       generate_subscripts(status, 1) as r from
		       -- join 5 minute intervals to the data
		       (select * from
		       	       (select tsrange(t1, t1 + interval '5 min') as t5m from generate_series('2017-04-25'::timestamp,'2017-04-26'::timestamp,'5 minutes'::interval) as t1) t
		     join (select * from (
		     	  -- now that we have wind estimates,
		     	  -- re-aggregate data (makes grouping into
		     	  -- 5min intervals much faster)
		     	  select lidar_id, scan_id, time,
			  	 array_agg(cnr) as cnr,
				 array_agg(drws) as drws,
				 array_agg(zwind) as zwind,
				 array_agg(status2) as status from (
			  	 select *,
				 	-- aggregate wind numbers into
				 	-- wind estimates where
				 	-- applicable
				        case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind from (
					-- unnest the raw data,
					-- calculate wind numbers
					select lidar_id, scan_id, time,
					       unnest(cnr) as cnr,
					       unnest(drws) as drws,
					       (case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0,
					       unnest(status) as status2,
					       generate_subscripts(status, 1) as rid
					from profiles
					where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z
				) z1
				group by lidar_id, scan_id, time
			) w1
		) p
		on t5m@>time
	) p1
) s1
group by lidar_id, scan_id, t5m, r order by r) cnr1
group by lidar_id, scan_id, t5m order by t5m;

-- 5min summary -- just zwind
-- aggregate again, get start times of 5 minute intervals
select lidar_id, scan_id, lower(t5m) as time, array_agg(zwind) as zwind from
-- get average of numbers where status is true
(select lidar_id, scan_id, t5m, r,
avg(case when status then zwind else null end) as zwind from
-- unnest everything again so we can get averages
(select lidar_id, scan_id, t5m,
unnest(zwind) as zwind, unnest(status) as status,
generate_subscripts(status, 1) as r from
-- join 5 minute intervals to the data
(select * from
(select tsrange(t1, t1 + interval '5 min') as t5m from generate_series('2017-04-25'::timestamp,'2017-04-26'::timestamp,'5 minutes'::interval) as t1) t
join (select * from (
-- now that we have wind estimates,
-- re-aggregate data (makes grouping into
-- 5min intervals much faster)
select lidar_id, scan_id, time,
array_agg(zwind) as zwind,
array_agg(status2) as status from (
select *,
-- aggregate wind numbers into
-- wind estimates where
-- applicable
case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind from (
-- unnest the raw data,
-- calculate wind numbers
select lidar_id, scan_id, time,
(case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0,
unnest(status) as status2,
generate_subscripts(status, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z
) z1
group by lidar_id, scan_id, time
) w1
) p
on t5m@>time
) p1
) s1
group by lidar_id, scan_id, t5m, r order by r) cnr1
group by lidar_id, scan_id, t5m order by t5m;

-- winds + 5min summary together-- v2-- this is actually slower!
select lower(t5m) as time, array_agg(cnr) as cnr, array_agg(drws) as drws, array_agg(zwind) as zwind from (select r, avg(case when status then cnr else null end) as cnr, avg(case when status then drws else null end) as drws, avg(case when status then zwind else null end) as zwind, t5m from (
select cnr, drws, zwind, status, rid as r, t5m from (
-- select unnest(cnr) as cnr, unnest(drws) as drws, unnest(zwind) as zwind, unnest(status) as status, generate_subscripts(status, 1) as r, t5m from (
select * from (select tsrange(t1, t1 + interval '5 min') as t5m from generate_series('2017-04-25'::timestamp,'2017-04-26'::timestamp,'5 minutes'::interval) as t1) t join (select * from
(select time, cnr, drws, zwind, status2 as status, rid
-- (select time, array_agg(cnr) as cnr, array_agg(drws) as drws, array_agg(zwind) as zwind, array_agg(status2) as status
from (
select *, case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind
from (
select lidar_id, scan_id, time, unnest(cnr) as cnr,
unnest(drws) as drws,
(case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0,
unnest(status) as status2, generate_subscripts(status, 1) as rid
from profiles
where time between '2017-04-25' and '2017-04-26' and lidar_id=27) z
) z1
) w1
) p on t5m@>time) p1) s1 group by r, t5m order by t5m, r asc) cnr1 group by t5m;

-- hmm...
create or replace view wind_profiles as
select lidar_id, scan_id, time, array_agg(zwind) as zwind from (select *, case when (count(*) filter (where status2) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following))=5 then sum(z0) over (partition by lidar_id, scan_id, rid rows between 2 preceding and 2 following) else null end as zwind from (select lidar_id, scan_id, time, (case when los_id=4 then (1/(4*sin(elevation)^2+1)) else (sin(elevation)/(4*sin(elevation)^2+1)) end)*unnest(rws) as z0, unnest(status) as status2, generate_subscripts(status, 1) as rid from profiles order by rid) z) z1 group by lidar_id, scan_id, time;

where time between '2017-04-25' and '2017-04-26' and lidar_id=27

-- timescaledb!
-- don't do this-
-- select distinct lidar_id from profiles where date(time)='2016-11-19';
-- Truncating to dates is sloooooow! Do this instead-
select distinct lidar_id from profiles where time between '2016-11-19' and '2016-11-20';
-- to drop-
drop table profiles cascade;

-- get available scans + scanning modes
select distinct lidar_id, scan_id from profiles where time between '2017-04-25' and '2017-04-26';




-- if you want to see netcdf files too:
-- create server lidar_nc_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.LidarNetcdf');
-- create foreign table lidar_netcdf (date date, site text, netcdf text) server lidar_nc_srv options(base '/web/html/private/lidar_netcdf/');
