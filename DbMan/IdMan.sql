
create table idman_generator (
	kind varchar(64) not null primary key,
	timed boolean not null,
	last_id bigint not null,
	ctime timestamp not null,	-- create time
	mtime timestamp not null on update CURRENT_TIMESTAMP,
	remark text not null,
);


--- The variable_setting table is the same table in DbMan.sql
create table variable_setting (
	name varchar(255) not null primary key,
	value varchar(255) not null,
	mtime timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
);

