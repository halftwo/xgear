
create table kind_setting (
	kind varchar(64) not null primary key,
	table_num int not null,
	table_prefix varchar(64) not null, 	-- if empty, kind is used
	id_field varchar(64) not null,
	enable bool not null default 1,
	version int not null,	--- change it when anything of this kind is changed
				--- including the table schema of this kind
	remark text not null,
	mtime timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
);

create table table_setting (
	kind varchar(64) not null,
	no int not null,
	sid int not null,
	db_name varchar(64) not null,

	unique (kind, no, sid)
);

create table server_setting (
	sid int not null primary key auto_increment,
	master_sid int not null,
	host varchar(255) not null,
	port int unsigned not null,
	user varchar(32) not null,
	passwd varchar(32) not null,
	active boolean not null default 1,
	remark text not null,

	index(master_sid),
	unique(host, port)
);

create table variable_setting (
	name varchar(255) not null primary key,
	value varchar(255) not null,
	mtime timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
);


insert into variable_setting(name, value) values('revision', 'The Initial Revision Not Changed Yet');
--- When anything of the kind_setting, table_setting, server_setting is changed, execute:
--- UPDATE variable_setting SET value='Change This to Another Value' WHERE name='revision';


