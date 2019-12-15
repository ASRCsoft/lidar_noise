-- tables used for HMRF research

create table status (scan_id smallint not null references scans, time timestamp not null, status boolean[], hmrf real[], primary key(scan_id, time));
-- make hypertable
select create_hypertable('status', 'time');

create table hmrf5m (scan_id smallint not null references scans, time timestamp not null, xwind real[], ywind real[], zwind real[], primary key(scan_id, time));
select create_hypertable('hmrf5m', 'time');
